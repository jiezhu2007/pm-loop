#!/usr/bin/env python3
"""Execute the bounded V4.5 G5 native Skill shadow migration.

The local Codex Skill directory remains the source of truth.  This runner
copies only ``SKILL.md`` into OpenViking's native agent Skill scope, records
every side-effect in the PM operation ledger, and reconciles response-unknown
by read-back. Legacy resource namespace deletion is a separately authorized
filesystem operation and is reported explicitly when verified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

import yaml

from pm_resource_dispatcher import is_legacy_skill_resource_uri
from pm_system_store import PMSystemStore, now_iso


TARGET_SCOPE = "viking://agent/skills"
LEGACY_SCOPE = "viking://resources/skills"
FRONTMATTER = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)\Z", re.DOTALL)


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _config() -> Dict[str, str]:
    values: Dict[str, str] = {}
    path = Path.home() / ".openviking" / "ovcli.conf"
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, Mapping):
                values.update({str(key): str(value) for key, value in loaded.items() if value is not None})
        except (OSError, ValueError):
            pass
    mapping = {
        "OPENVIKING_URL": "url",
        "OPENVIKING_API_KEY": "api_key",
        "OPENVIKING_ACCOUNT": "account",
        "OPENVIKING_USER": "user",
    }
    for source, target in mapping.items():
        if os.environ.get(source):
            values[target] = str(os.environ[source])
    values["url"] = str(values.get("url") or "http://127.0.0.1:1933").rstrip("/")
    return values


def _headers(config: Mapping[str, str]) -> Dict[str, str]:
    result = {"Accept": "application/json", "Content-Type": "application/json"}
    if config.get("api_key"):
        result["Authorization"] = f"Bearer {config['api_key']}"
    if config.get("account"):
        result["X-OpenViking-Account"] = str(config["account"])
    if config.get("user"):
        result["X-OpenViking-User"] = str(config["user"])
    return result


class SkillAPI:
    def __init__(self, *, timeout: float = 90.0) -> None:
        self.config = _config()
        self.timeout = min(180.0, max(5.0, float(timeout)))

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Mapping[str, Any]] = None,
        query: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        url = self.config["url"] + path
        if query:
            url += "?" + urllib.parse.urlencode({key: value for key, value in query.items() if value is not None})
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        request = urllib.request.Request(url, data=data, headers=_headers(self.config), method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout or self.timeout) as response:
                raw = response.read()
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                return {"http_status": int(response.status), "payload": payload, "response_state": "received"}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = {"raw": raw[-2000:]}
            return {"http_status": int(exc.code), "payload": payload, "response_state": "received"}
        except (TimeoutError, urllib.error.URLError) as exc:
            return {"http_status": None, "payload": {}, "response_state": "unknown", "error": f"{type(exc).__name__}: {exc}"}

    def list(self, target_scope: str = TARGET_SCOPE) -> Dict[str, Any]:
        return self.request("GET", "/api/v1/skills", query={"target_uri": target_scope, "node_limit": 1000})

    def get(self, name: str, target_scope: str = TARGET_SCOPE) -> Dict[str, Any]:
        return self.request(
            "GET",
            f"/api/v1/skills/{urllib.parse.quote(name, safe='')}",
            query={
                "target_uri": target_scope,
                "include_content": "true",
                "include_files": "true",
                "include_integrity": "true",
                "include_source": "true",
            },
        )


def _canonical_skill(content: str) -> Dict[str, Any]:
    match = FRONTMATTER.match(str(content))
    if not match:
        raise ValueError("SKILL.md must contain YAML frontmatter")
    meta = yaml.safe_load(match.group(1)) or {}
    if not isinstance(meta, Mapping):
        raise ValueError("SKILL.md frontmatter must be a mapping")
    name = str(meta.get("name") or "").strip()
    description = str(meta.get("description") or "").strip()
    if not name or not description:
        raise ValueError("SKILL.md requires name and description")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name):
        raise ValueError(f"invalid native Skill name: {name}")
    tools = meta.get("allowed-tools", meta.get("allowed_tools", []))
    if isinstance(tools, str):
        tools = [item for item in re.split(r"[\s,]+", tools.strip()) if item]
    elif not isinstance(tools, list):
        tools = []
    tags = meta.get("tags") or []
    if not isinstance(tags, list):
        tags = [tags]
    return {
        "name": name,
        "description": description,
        "allowed_tools": [str(item) for item in tools],
        "tags": [str(item) for item in tags],
        "content": match.group(2).strip(),
    }


def build_source_manifest(skill_root: Path, *, epoch: str, target_scope: str) -> Dict[str, Any]:
    skills: list[Dict[str, Any]] = []
    for directory in sorted(Path(skill_root).expanduser().resolve().iterdir()):
        source = directory / "SKILL.md"
        if not directory.is_dir() or not source.is_file():
            continue
        raw = source.read_bytes()
        text = raw.decode("utf-8")
        canonical = _canonical_skill(text)
        source_files = sorted(
            str(item.relative_to(directory))
            for item in directory.rglob("*")
            if item.is_file() and not any(part in {"state", "output", "outputs", "cache", "tmp", "__pycache__"} for part in item.relative_to(directory).parts)
        )
        record = {
            "name": canonical["name"],
            "directory_name": directory.name,
            "name_matches_directory": canonical["name"] == directory.name,
            "source_path": str(source),
            "source_sha256": _hash_bytes(raw),
            "canonical_hash": _hash_json(canonical),
            "source_bytes": len(raw),
            "source_file_count": len(source_files),
            "minimal_package_files": ["SKILL.md"],
            "minimal_package_hash": _hash_bytes(raw),
            "target_uri": f"{target_scope}/{canonical['name']}",
            "namespace_epoch": epoch,
        }
        skills.append(record)
    manifest = {
        "schema": "pm-system.v45-r2-g5-source-manifest.v1",
        "captured_at": now_iso(),
        "source_root": str(Path(skill_root).expanduser().resolve()),
        "target_scope": target_scope,
        "legacy_scope": LEGACY_SCOPE,
        "namespace_epoch": epoch,
        "skill_count": len(skills),
        "skills": skills,
    }
    names = [str(item["name"]) for item in skills]
    if len(names) != len(set(names)):
        duplicates = sorted(name for name in set(names) if names.count(name) > 1)
        raise ValueError(f"duplicate native Skill names: {duplicates}")
    manifest["manifest_hash"] = "sha256:" + _hash_json(manifest)
    return manifest


def _result(envelope: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping) or str(payload.get("status")) != "ok":
        return {}
    result = payload.get("result")
    return result if isinstance(result, Mapping) else {}


def _remote_hash(envelope: Mapping[str, Any]) -> Optional[str]:
    result = _result(envelope)
    content = result.get("content")
    if not isinstance(content, str):
        return None
    try:
        return _hash_json(_canonical_skill(content))
    except (TypeError, ValueError, yaml.YAMLError):
        return None


def _summarize(envelope: Mapping[str, Any]) -> Dict[str, Any]:
    result = _result(envelope)
    return {
        "http_status": envelope.get("http_status"),
        "response_state": envelope.get("response_state"),
        "error": envelope.get("error"),
        "status": (envelope.get("payload") or {}).get("status") if isinstance(envelope.get("payload"), Mapping) else None,
        "name": result.get("name"),
        "uri": result.get("uri") or result.get("root_uri"),
        "task_id": result.get("task_id"),
        "total": result.get("total"),
        "valid": result.get("valid"),
        "revision": result.get("revision"),
        "estimated_deleted_count": result.get("estimated_deleted_count"),
    }


def _operation(
    store: PMSystemStore,
    api: SkillAPI,
    *,
    operation: str,
    name: str,
    content: Optional[str],
    epoch: str,
    target_scope: str,
    attempt: int,
) -> Dict[str, Any]:
    method = {"add": "POST", "update": "PUT", "delete": "DELETE"}[operation]
    path = "/api/v1/skills" if operation == "add" else f"/api/v1/skills/{urllib.parse.quote(name, safe='')}"
    body: Optional[Dict[str, Any]] = None
    query: Optional[Dict[str, Any]] = None
    if operation in {"add", "update"}:
        body = {
            "data": content,
            "wait": False,
            "target_uri": target_scope,
            "source_metadata": {
                "type": "pm-v45-r2-migration",
                "migration_id": epoch,
                "namespace_epoch": epoch,
                "target_scope": target_scope,
                "skill_name": name,
                "tracked": True,
            },
        }
    else:
        query = {"target_uri": target_scope}
    request_hash = _hash_json({"method": method, "path": path, "body": body, "query": query})
    ledger = store.begin_operation(
        operation_type=f"skill.{operation}",
        idempotency_key=f"{name}|{target_scope}|{epoch}",
        target_uri=f"{target_scope}/{name}",
        request_hash=request_hash,
        namespace_epoch=epoch,
        attempt=attempt,
    )
    if ledger.get("deduplicated") and ledger.get("response_state") == "completed":
        return {"ledger": ledger, "deduplicated": True, "response": json.loads(str(ledger.get("response_json") or "{}"))}
    response = api.request(method, path, body=body, query=query)
    state = "accepted" if response.get("response_state") == "received" and response.get("http_status") in {200, 201, 202} else "unknown" if response.get("response_state") == "unknown" else "failed"
    summary = _summarize(response)
    store.finish_operation(str(ledger["operation_id"]), response_state=state, response=summary)
    return {"ledger": ledger, "deduplicated": False, "response": summary, "raw": response}


def _reconcile(
    api: SkillAPI,
    *,
    name: str,
    canonical_hash: str,
    target_scope: str,
    deadline_seconds: float,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(deadline_seconds))
    observations: list[Dict[str, Any]] = []
    while True:
        current = api.get(name, target_scope)
        observed_hash = _remote_hash(current)
        observations.append({**_summarize(current), "canonical_hash": observed_hash})
        if observed_hash == canonical_hash:
            return {"matched": True, "observations": observations, "last": current}
        if time.monotonic() >= deadline:
            return {"matched": False, "observations": observations, "last": current}
        time.sleep(2.0)


def _finish_operation(store: PMSystemStore, operation: Mapping[str, Any], *, state: str, evidence: Mapping[str, Any]) -> None:
    ledger = operation.get("ledger") or {}
    operation_id = ledger.get("operation_id")
    if operation_id:
        store.finish_operation(str(operation_id), response_state=state, response=evidence)


def sync_skill(
    store: PMSystemStore,
    api: SkillAPI,
    record: Mapping[str, Any],
    *,
    epoch: str,
    target_scope: str,
    reconcile_seconds: float,
) -> Dict[str, Any]:
    name = str(record["name"])
    source = Path(str(record["source_path"])).read_text(encoding="utf-8")
    expected_hash = str(record["canonical_hash"])
    before = api.get(name, target_scope)
    before_hash = _remote_hash(before)
    if before_hash == expected_hash:
        return {"name": name, "status": "retained", "canonical_hash": expected_hash, "before": _summarize(before)}
    operation_name = "update" if before.get("http_status") == 200 else "add"
    attempts: list[Dict[str, Any]] = []
    for attempt in (1, 2):
        operation = _operation(
            store,
            api,
            operation=operation_name,
            name=name,
            content=source,
            epoch=epoch,
            target_scope=target_scope,
            attempt=attempt,
        )
        reconciliation = _reconcile(
            api,
            name=name,
            canonical_hash=expected_hash,
            target_scope=target_scope,
            deadline_seconds=reconcile_seconds,
        )
        evidence = {
            "operation": operation_name,
            "attempt": attempt,
            "transport": operation.get("response"),
            "matched": reconciliation["matched"],
            "observations": reconciliation["observations"],
        }
        attempts.append(evidence)
        if reconciliation["matched"]:
            _finish_operation(store, operation, state="completed", evidence=evidence)
            return {"name": name, "status": "synced", "canonical_hash": expected_hash, "attempts": attempts}
        _finish_operation(store, operation, state="unknown" if attempt == 1 else "quarantine", evidence=evidence)
        if attempt == 1:
            last_status = (reconciliation.get("last") or {}).get("http_status")
            if last_status not in {404, None}:
                break
    return {"name": name, "status": "quarantine", "canonical_hash": expected_hash, "attempts": attempts}


def validate_all(api: SkillAPI, manifest: Mapping[str, Any], *, target_scope: str) -> list[Dict[str, Any]]:
    results: list[Dict[str, Any]] = []
    for record in manifest.get("skills", []):
        content = Path(str(record["source_path"])).read_text(encoding="utf-8")
        response = api.request(
            "POST",
            "/api/v1/skills/validate",
            body={"data": content, "strict": True, "skill_dir_name": record["name"], "target_uri": target_scope},
        )
        result = _result(response)
        results.append({"name": record["name"], "valid": bool(result.get("valid")), "summary": _summarize(response), "errors": result.get("errors") or [], "warnings": result.get("warnings") or []})
    return results


def run_canary(
    store: PMSystemStore,
    api: SkillAPI,
    *,
    epoch: str,
    target_scope: str,
    source: Path,
    reconcile_seconds: float,
) -> Dict[str, Any]:
    canary_name = f"v45-r2-native-canary-{int(time.time())}"
    original = source.read_text(encoding="utf-8")
    canonical = _canonical_skill(original)
    frontmatter, body = FRONTMATTER.match(original).groups()  # type: ignore[union-attr]
    meta = yaml.safe_load(frontmatter)
    meta["name"] = canary_name
    content = "---\n" + yaml.safe_dump(meta, allow_unicode=True, sort_keys=False) + "---\n\n" + body.strip() + "\n"
    expected_hash = _hash_json(_canonical_skill(content))
    validate = api.request("POST", "/api/v1/skills/validate", body={"data": content, "strict": True, "skill_dir_name": canary_name, "target_uri": target_scope})
    add = _operation(store, api, operation="add", name=canary_name, content=content, epoch=epoch, target_scope=target_scope, attempt=1)
    read_back = _reconcile(api, name=canary_name, canonical_hash=expected_hash, target_scope=target_scope, deadline_seconds=reconcile_seconds)
    _finish_operation(store, add, state="completed" if read_back["matched"] else "quarantine", evidence={"canary": "add", "matched": read_back["matched"], "observations": read_back["observations"]})

    updated_content = content.rstrip() + "\n\n<!-- v45-r2-native-canary-update -->\n"
    updated_hash = _hash_json(_canonical_skill(updated_content))
    update = _operation(store, api, operation="update", name=canary_name, content=updated_content, epoch=epoch, target_scope=target_scope, attempt=1)
    update_read_back = _reconcile(api, name=canary_name, canonical_hash=updated_hash, target_scope=target_scope, deadline_seconds=reconcile_seconds)
    _finish_operation(store, update, state="completed" if update_read_back["matched"] else "quarantine", evidence={"canary": "update", "matched": update_read_back["matched"], "observations": update_read_back["observations"]})

    find: Dict[str, Any] = {}
    find_names: set[str] = set()
    find_observations: list[Dict[str, Any]] = []
    find_deadline = time.monotonic() + max(1.0, float(reconcile_seconds))
    while True:
        find = api.request("POST", "/api/v1/skills/find", body={"query": canary_name, "limit": 10, "target_uri": target_scope})
        find_names = {str(item.get("name")) for item in (_result(find).get("skills") or []) if isinstance(item, Mapping)}
        find_observations.append({"names": sorted(find_names), "summary": _summarize(find)})
        if canary_name in find_names or time.monotonic() >= find_deadline:
            break
        time.sleep(2.0)
    delete = _operation(store, api, operation="delete", name=canary_name, content=None, epoch=epoch, target_scope=target_scope, attempt=1)
    final = api.list(target_scope)
    final_names = {str(item.get("name")) for item in (_result(final).get("skills") or []) if isinstance(item, Mapping)}
    deleted = canary_name not in final_names and delete.get("response", {}).get("http_status") == 200
    delete_find_observations: list[Dict[str, Any]] = []
    delete_find_deadline = time.monotonic() + max(1.0, float(reconcile_seconds))
    while deleted and time.monotonic() < delete_find_deadline:
        probe = api.request("POST", "/api/v1/skills/find", body={"query": canary_name, "limit": 10, "target_uri": target_scope})
        probe_names = {str(item.get("name")) for item in (_result(probe).get("skills") or []) if isinstance(item, Mapping)}
        delete_find_observations.append({"names": sorted(probe_names), "summary": _summarize(probe)})
        if canary_name not in probe_names:
            break
        time.sleep(2.0)
    deleted = deleted and (not delete_find_observations or canary_name not in delete_find_observations[-1]["names"])
    _finish_operation(store, delete, state="completed" if deleted else "quarantine", evidence={"canary": "delete", "deleted": deleted, "final_names": sorted(final_names)})
    passed = bool(_result(validate).get("valid")) and read_back["matched"] and update_read_back["matched"] and canary_name in find_names and deleted
    return {
        "name": canary_name,
        "target_scope": target_scope,
        "validate": _summarize(validate),
        "add_read_back": {"matched": read_back["matched"], "observations": read_back["observations"]},
        "update_read_back": {"matched": update_read_back["matched"], "observations": update_read_back["observations"]},
        "find": {**_summarize(find), "names": sorted(find_names), "observations": find_observations},
        "delete": {"transport": delete.get("response"), "deleted": deleted, "final_names": sorted(final_names), "find_observations": delete_find_observations},
        "passed": passed,
    }


def run_g5(
    *,
    db_path: Path,
    skill_root: Path,
    output: Path,
    epoch: str,
    target_scope: str = TARGET_SCOPE,
    apply: bool = False,
    run_canary_smoke: bool = False,
    timeout: float = 90.0,
    reconcile_seconds: float = 90.0,
    legacy_physical_delete_verified: bool = False,
) -> Dict[str, Any]:
    store = PMSystemStore(db_path)
    freeze = store.migration_freeze()
    manifest = build_source_manifest(skill_root, epoch=epoch, target_scope=target_scope)
    api = SkillAPI(timeout=timeout)
    before = api.list(target_scope)
    validations = validate_all(api, manifest, target_scope=target_scope)
    canary = run_canary(
        store,
        api,
        epoch=epoch,
        target_scope=target_scope,
        source=Path(str(manifest["skills"][0]["source_path"])),
        reconcile_seconds=reconcile_seconds,
    ) if run_canary_smoke else None

    synced: list[Dict[str, Any]] = []
    if apply and all(item["valid"] for item in validations) and (canary is None or canary["passed"]):
        for record in manifest["skills"]:
            synced.append(
                sync_skill(
                    store,
                    api,
                    record,
                    epoch=epoch,
                    target_scope=target_scope,
                    reconcile_seconds=reconcile_seconds,
                )
            )
            output.parent.mkdir(parents=True, exist_ok=True)
            checkpoint = {
                "schema": "pm-system.v45-r2-g5-progress.v1",
                "updated_at": now_iso(),
                "namespace_epoch": epoch,
                "target_scope": target_scope,
                "source_manifest_hash": manifest["manifest_hash"],
                "skills": synced,
            }
            output.with_name("g5-skill-progress.json").write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    after = api.list(target_scope)
    after_names = {str(item.get("name")) for item in (_result(after).get("skills") or []) if isinstance(item, Mapping)}
    expected_names = {str(item["name"]) for item in manifest["skills"]}
    operation_rows = store.list_operations(operation_type="skill.add", limit=1000) + store.list_operations(operation_type="skill.update", limit=1000) + store.list_operations(operation_type="skill.delete", limit=1000)
    sync_failures = [item for item in synced if item.get("status") == "quarantine"]
    validation_failures = [item for item in validations if not item["valid"]]
    source_freeze = bool(freeze and freeze.get("state") == "freeze" and freeze.get("migration_epoch") == epoch)
    # Historical native orphans are retained as evidence; every current local
    # Skill must be present, but an extra registered orphan must not fail sync.
    extra_names = sorted(after_names - expected_names)
    shadow_complete = bool(apply and not validation_failures and not sync_failures and expected_names <= after_names)
    legacy_fence = is_legacy_skill_resource_uri(LEGACY_SCOPE)
    report = {
        "schema": "pm-system.v45-r2-g5-manifest.v1",
        "captured_at": now_iso(),
        "namespace_epoch": epoch,
        "target_scope": target_scope,
        "source_freeze": source_freeze,
        "source_manifest": manifest,
        "native_before": _summarize(before),
        "validations": validations,
        "canary": canary,
        "sync_results": synced,
        "native_after": {**_summarize(after), "names": sorted(after_names)},
        "expected_names": sorted(expected_names),
        "extra_names": extra_names,
        "shadow_complete": shadow_complete,
        "legacy_namespace": {
            "uri": LEGACY_SCOPE,
            "enqueue_fenced": legacy_fence,
            "physical_delete_performed": bool(legacy_physical_delete_verified),
            "delete_authorization_required": not legacy_physical_delete_verified,
            "state": "deleted" if legacy_physical_delete_verified else "retained_for_rollback",
        },
        "operation_ledger": {
            "count": len(operation_rows),
            "unknown": sum(1 for item in operation_rows if item.get("response_state") == "unknown"),
            "quarantine": sum(1 for item in operation_rows if item.get("response_state") == "quarantine"),
        },
        "decision": "PASS" if shadow_complete and legacy_fence and legacy_physical_delete_verified and (canary is None or canary["passed"]) else "READY_FOR_DELETE_CONFIRMATION" if shadow_complete and legacy_fence and (canary is None or canary["passed"]) else "HOLD",
    }
    report["manifest_hash"] = "sha256:" + _hash_json(report)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--skill-root", type=Path, default=Path.home() / ".codex" / "skills")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--target-scope", default=TARGET_SCOPE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--canary", action="store_true")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--reconcile-seconds", type=float, default=90.0)
    parser.add_argument(
        "--legacy-physical-delete-verified",
        action="store_true",
        help="mark the separately authorized legacy namespace filesystem deletion as verified",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_g5(
        db_path=args.db_path,
        skill_root=args.skill_root,
        output=args.output,
        epoch=args.epoch,
        target_scope=args.target_scope,
        apply=args.apply,
        run_canary_smoke=args.canary,
        timeout=args.timeout,
        reconcile_seconds=args.reconcile_seconds,
        legacy_physical_delete_verified=args.legacy_physical_delete_verified,
    )
    print(json.dumps({"decision": result["decision"], "manifest_hash": result["manifest_hash"], "output": str(args.output)}, ensure_ascii=False))
    return 0 if result["decision"] in {"PASS", "READY_FOR_DELETE_CONFIRMATION"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
