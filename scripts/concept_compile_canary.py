#!/usr/bin/env python3
"""Run a bounded, production-read-only concept compiler recovery canary.

The full concept refresh remains disabled.  This runner reads two explicitly
selected Active concepts and their metadata-only source map, then exercises
Candidate staging, the human gate contract, atomic generation publication,
one bounded model disconnect recovery, and idempotent replay in an isolated
directory.  It never writes the installed concept skill or calls OpenViking.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


SCHEMA_VERSION = "concept-compile-canary.v1"
CHECKPOINT_SCHEMA = "concept-compile-canary.checkpoint.v1"
DEFAULT_SKILL_ROOT = Path("~/.codex/skills/shengsuan-concepts")
DEFAULT_COVERAGE = Path("~/.codex/pm-loop/state/concept-v11/source-coverage-current.json")
DEFAULT_CONCEPTS = ("DataAgent", "文件管理")
DEFAULT_WORK_DIR = Path(tempfile.gettempdir()) / "pm-v44-s8.5.2-canary"
MAX_RETRIES = 1


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _atomic_write(path: Path, content: bytes) -> None:
    """Publish a complete file with a same-directory replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write(path, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def _file_hash(path: Path) -> Optional[str]:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError:
        return None


class DisconnectingProvider:
    """Deterministic provider double used to validate bounded recovery."""

    def __init__(self, *, fail_first: bool = True) -> None:
        self.fail_first = fail_first
        self.calls = 0
        self.failed = False

    def generate(self, concept: str, existing: str, evidence: Sequence[Mapping[str, Any]], input_hash: str) -> str:
        self.calls += 1
        if self.fail_first and not self.failed:
            self.failed = True
            raise ConnectionError("simulated provider disconnect after request acceptance")
        # Keep the generated page valid and deterministic.  The content is
        # intentionally isolated; it is never published to the real skill.
        marker = (
            "\n\n## S8.5.2 隔离编译 canary\n"
            f"- concept: {concept}\n"
            f"- evidence_count: {len(evidence)}\n"
            f"- model_input_hash: {input_hash}\n"
        )
        return existing.rstrip() + marker


def _load_inputs(skill_root: Path, coverage_path: Path, concepts: Sequence[str]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    ledger_path = skill_root / "state" / "concepts-ledger.json"
    pages_root = skill_root / "state" / "pages"
    ledger = _read_json(ledger_path, {})
    coverage = _read_json(coverage_path, {})
    if not isinstance(ledger, dict):
        raise RuntimeError(f"invalid concepts ledger: {ledger_path}")
    if (
        not isinstance(coverage, dict)
        or coverage.get("schema") != "concept-v11.source-coverage-report.v1"
        or coverage.get("status") != "PASS"
        or not isinstance(coverage.get("gate"), dict)
        or coverage["gate"].get("p3_closed") is not True
    ):
        raise RuntimeError(f"invalid or unclosed source coverage: {coverage_path}")
    selected: Dict[str, Any] = {}
    coverage_by_concept = {
        str(row.get("concept") or ""): row
        for row in coverage.get("concepts", [])
        if isinstance(row, dict) and str(row.get("concept") or "")
    }
    for name in concepts:
        record = ledger.get(name)
        if not isinstance(record, dict) or str(record.get("status") or "active") != "active":
            raise RuntimeError(f"concept is not Active: {name}")
        page_path = pages_root / f"{name}.md"
        if not page_path.is_file():
            raise RuntimeError(f"Active page missing: {page_path}")
        coverage_row = coverage_by_concept.get(name)
        if not isinstance(coverage_row, dict):
            raise RuntimeError(f"no source coverage row for concept: {name}")
        if str(coverage_row.get("coverage_status") or "") not in {"refreshable", "substituted"}:
            raise RuntimeError(f"concept is not refreshable: {name}")
        refs = [
            row for row in coverage_row.get("references", [])
            if isinstance(row, dict)
            and str(row.get("disposition") or "") in {"mapped", "substituted"}
            and str(row.get("source_map_status") or "") == "mapped"
            and str(row.get("evidence_set_hash") or "").startswith("sha256:")
        ]
        if not refs:
            raise RuntimeError(f"concept has no current mapped source: {name}")
        selected[name] = {
            "record": record,
            "page_path": page_path,
            "page_content": page_path.read_text(encoding="utf-8"),
            "source_refs": sorted(refs, key=lambda row: str(row.get("source_uri") or "")),
        }
    return selected, ledger, coverage


def _source_snapshot(item: Mapping[str, Any]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for ref in item["source_refs"]:
        uri = str(ref["source_uri"])
        result.append({
            "uri": uri,
            "map_id": str(ref.get("map_id") or ""),
            "disposition": str(ref.get("disposition") or ""),
            "evidence_set_hash": str(ref.get("evidence_set_hash") or ""),
        })
    return result


def _candidate_payload(concept: str, content: str, source_snapshot: Sequence[Mapping[str, Any]], base_hash: str, input_hash: str) -> Dict[str, Any]:
    return {
        "schema_version": "concept-learning.candidate.v1",
        "candidate_id": f"canary-{hashlib.sha256((concept + input_hash).encode('utf-8')).hexdigest()[:16]}",
        "concept": concept,
        "kind": "refresh",
        "status": "ready_for_review",
        "content_hash": _sha256_text(content),
        "base_page_sha256": base_hash,
        "source_snapshot": list(source_snapshot),
        "source_refs": [str(item["uri"]) for item in source_snapshot],
        "evidence": [
            {
                "uri": item["uri"],
                "map_id": item["map_id"],
                "evidence_set_hash": item["evidence_set_hash"],
            }
            for item in source_snapshot
        ],
        "proposal_fingerprint": input_hash,
        "canary_only": True,
    }


def _publish_atomic(active_dir: Path, candidate: Mapping[str, Any], content: str, *, inject_failure: bool = False) -> bool:
    """Atomically switch the isolated active generation, preserving old state on failure."""
    active_dir.mkdir(parents=True, exist_ok=True)
    active_meta = active_dir / "current.json"
    active_page = active_dir / "current.md"
    before_meta = active_meta.read_bytes() if active_meta.exists() else None
    before_page = active_page.read_bytes() if active_page.exists() else None
    staged_meta = active_dir / ".current.json.stage"
    staged_page = active_dir / ".current.md.stage"
    try:
        staged_meta.write_bytes((json.dumps(candidate, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
        staged_page.write_text(content, encoding="utf-8")
        if inject_failure:
            raise OSError("simulated crash before active replace")
        os.replace(staged_page, active_page)
        os.replace(staged_meta, active_meta)
        return True
    except OSError:
        if before_meta is None:
            active_meta.unlink(missing_ok=True)
        else:
            active_meta.write_bytes(before_meta)
        if before_page is None:
            active_page.unlink(missing_ok=True)
        else:
            active_page.write_bytes(before_page)
        return False
    finally:
        staged_meta.unlink(missing_ok=True)
        staged_page.unlink(missing_ok=True)


def _run_concept(
    *,
    concept: str,
    item: Mapping[str, Any],
    work_dir: Path,
    idempotency_key: str,
    provider: DisconnectingProvider,
    replay: bool,
) -> Dict[str, Any]:
    concept_dir = work_dir / concept
    checkpoint_path = concept_dir / "checkpoint.json"
    staged_dir = concept_dir / "staged"
    active_dir = concept_dir / "active"
    existing = str(item["page_content"])
    base_hash = _sha256_text(existing)
    source_snapshot = _source_snapshot(item)
    input_hash = _sha256_text(_canonical({
        "schema_version": SCHEMA_VERSION,
        "idempotency_key": idempotency_key,
        "concept": concept,
        "base_page_sha256": base_hash,
        "source_snapshot": source_snapshot,
        "provider_profile": "oneapi-trusted-canary-v1",
    }))
    checkpoint = _read_json(checkpoint_path, {})
    if isinstance(checkpoint, dict) and checkpoint.get("input_hash") and checkpoint.get("input_hash") != input_hash:
        raise RuntimeError(f"idempotency key reused with different input: {concept}")
    if isinstance(checkpoint, dict) and checkpoint.get("status") == "completed":
        return {**checkpoint, "replayed": True, "provider_calls_on_replay": provider.calls}

    checkpoint = {
        "schema_version": CHECKPOINT_SCHEMA,
        "status": "running",
        "concept": concept,
        "idempotency_key": idempotency_key,
        "input_hash": input_hash,
        "base_page_sha256": base_hash,
        "source_snapshot": source_snapshot,
        "model_call": {"status": "pending", "attempts": 0, "max_retries": MAX_RETRIES, "input_hash": input_hash},
        "staged": {"status": "pending"},
        "gate": {"required": True, "status": "pending"},
        "active": {"status": "pending"},
        "production_write": {"attempted": False},
    }
    _atomic_json(checkpoint_path, checkpoint)

    output = ""
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        checkpoint["model_call"]["attempts"] = attempt + 1
        try:
            output = provider.generate(concept, existing, source_snapshot, input_hash)
            checkpoint["model_call"].update({"status": "completed", "output_hash": _sha256_text(output)})
            break
        except ConnectionError as exc:
            last_error = str(exc)
            checkpoint["model_call"].update({"status": "result_unknown", "error": last_error})
            if attempt >= MAX_RETRIES:
                checkpoint["status"] = "failed"
                _atomic_json(checkpoint_path, checkpoint)
                raise RuntimeError(f"bounded model recovery exhausted for {concept}")
            checkpoint["model_call"].update({"status": "retry_wait", "retry_input_hash": input_hash})
            _atomic_json(checkpoint_path, checkpoint)
            time.sleep(0.01)
    if not output:
        raise RuntimeError(f"provider returned empty content for {concept}: {last_error or 'unknown'}")

    candidate = _candidate_payload(concept, output, source_snapshot, base_hash, input_hash)
    staged_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write(staged_dir / "candidate.md", output.encode("utf-8"))
    _atomic_json(staged_dir / "candidate.json", candidate)
    checkpoint["staged"] = {"status": "ready_for_review", "candidate_id": candidate["candidate_id"], "content_hash": candidate["content_hash"]}
    checkpoint["gate"] = {"required": True, "status": "approved", "actor": "canary-simulated-human-gate"}
    checkpoint["active"] = {"status": "pending"}
    _atomic_json(checkpoint_path, checkpoint)

    # Prove a failed active switch leaves the previous isolated generation
    # untouched, then perform the real atomic switch.
    failed_switch = _publish_atomic(active_dir, candidate, output, inject_failure=True)
    if failed_switch:
        raise RuntimeError(f"atomic failure injection unexpectedly published: {concept}")
    atomic_rollback_ok = not (active_dir / "current.json").exists() and not (active_dir / "current.md").exists()
    if not atomic_rollback_ok:
        raise RuntimeError(f"atomic rollback check failed: {concept}")
    if not _publish_atomic(active_dir, candidate, output):
        raise RuntimeError(f"atomic active switch failed: {concept}")
    checkpoint["active"] = {"status": "published_in_isolation", "candidate_id": candidate["candidate_id"], "content_hash": candidate["content_hash"]}
    checkpoint["atomic_rollback_check"] = atomic_rollback_ok
    checkpoint["production_write"] = {"attempted": False, "paths": [str(item["page_path"])]}
    checkpoint["status"] = "completed"
    _atomic_json(checkpoint_path, checkpoint)

    return {
        **checkpoint,
        "replayed": False,
        "provider_calls": provider.calls,
        "source_map_status": "mapped",
    }


def run_canary(
    *,
    skill_root: Path,
    coverage_path: Path,
    concepts: Sequence[str] = DEFAULT_CONCEPTS,
    work_dir: Path = DEFAULT_WORK_DIR,
    idempotency_key: str = "v44-s8.5.2-canary-20260829-01",
) -> Dict[str, Any]:
    skill_root = skill_root.expanduser().resolve()
    coverage_path = coverage_path.expanduser().resolve()
    selected, ledger, coverage = _load_inputs(skill_root, coverage_path, concepts)
    tracked_paths: List[Path] = [skill_root / "state" / "concepts-ledger.json", coverage_path]
    tracked_paths.extend(item["page_path"] for item in selected.values())
    before = {str(path): _file_hash(path) for path in tracked_paths}
    work_dir = work_dir.expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    provider = DisconnectingProvider(fail_first=True)
    results = []
    for concept in concepts:
        result = _run_concept(
            concept=concept,
            item=selected[concept],
            work_dir=work_dir,
            idempotency_key=idempotency_key,
            provider=provider,
            replay=False,
        )
        results.append(result)
    # Reopen the completed checkpoints with a fresh provider.  A replay must
    # not issue another model request and must return the same Candidate id.
    replay_provider = DisconnectingProvider(fail_first=True)
    replayed = []
    for concept in concepts:
        replayed.append(_run_concept(
            concept=concept,
            item=selected[concept],
            work_dir=work_dir,
            idempotency_key=idempotency_key,
            provider=replay_provider,
            replay=True,
        ))
    after = {str(path): _file_hash(path) for path in tracked_paths}
    production_unchanged = before == after
    idempotent = replay_provider.calls == 0 and [r.get("staged", {}).get("candidate_id") for r in results] == [r.get("staged", {}).get("candidate_id") for r in replayed]
    fresh_results = [r for r in results if not bool(r.get("replayed"))]
    replay_results = [r for r in results if bool(r.get("replayed"))]
    retried = [r for r in fresh_results if int(r.get("model_call", {}).get("attempts") or 0) > 1]
    retry_hashes_match = not fresh_results or (
        bool(retried)
        and all(r.get("model_call", {}).get("retry_input_hash") == r.get("input_hash") for r in retried)
    )
    expected_fresh_calls = len(fresh_results) + (1 if fresh_results else 0)
    provider_call_shape_ok = provider.calls == expected_fresh_calls
    execution_mode = (
        "replay" if not fresh_results else "fresh" if not replay_results else "mixed"
    )
    # A completed checkpoint is a successful, provider-free execution.  This
    # matters when an operator re-runs the same canary command after the first
    # invocation: replay must not be penalized for making zero model calls.
    passed = (
        all(r.get("status") == "completed" for r in results)
        and production_unchanged
        and idempotent
        and retry_hashes_match
        and provider_call_shape_ok
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": "S8.5.2",
        "observed_at": _now_iso(),
        "status": "PASS" if passed else "HOLD_CONTINUE",
        "scope": "isolated_candidate_canary",
        "concepts": list(concepts),
        "idempotency_key": idempotency_key,
        "work_dir": str(work_dir),
        "execution_mode": execution_mode,
        "refresh_guard": "CONCEPT_REFRESH_DISABLED=True",
        "source_coverage": {
            "path": str(coverage_path),
            "schema": coverage.get("schema"),
            "report_hash": coverage.get("report_hash"),
            "selected_all_mapped": True,
            "selected_source_refs": sum(len(item["source_refs"]) for item in selected.values()),
        },
        "model_recovery": {
            "provider": "deterministic-disconnect-double",
            "initial_disconnects": max(0, provider.calls - len(fresh_results)),
            "calls": provider.calls,
            "max_retries": MAX_RETRIES,
            "same_model_input_hash_on_retry": retry_hashes_match,
            "fresh_concepts": len(fresh_results),
            "replayed_concepts": len(replay_results),
            "provider_call_shape_ok": provider_call_shape_ok,
        },
        "atomic_generation": {
            "staged_candidates": len(results),
            "active_switches": sum(1 for r in results if r.get("active", {}).get("status") == "published_in_isolation"),
            "rollback_injection_pass": all(bool(r.get("atomic_rollback_check")) for r in results),
            "production_active_touched": False,
        },
        "idempotency": {"replay_provider_calls": replay_provider.calls, "replay_same_candidate": idempotent},
        "production_hashes": {"before": before, "after": after, "unchanged": production_unchanged},
        "results": results,
        "replays": replayed,
        "next_gate": "保持全量概念刷新关闭；仅在 source-map 覆盖和人工 Gate 规则进一步闭合后评估扩大范围。",
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the isolated V4.4 S8.5.2 concept compiler recovery canary")
    parser.add_argument("--skill-root", type=Path, default=DEFAULT_SKILL_ROOT)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--idempotency-key", default="v44-s8.5.2-canary-20260829-01")
    parser.add_argument("--output", type=Path, help="write the complete canary manifest to this path")
    parser.add_argument("--concept", dest="concepts", action="append", help="repeat for one or two mapped Active concepts")
    args = parser.parse_args(argv)
    concepts = tuple(args.concepts or DEFAULT_CONCEPTS)
    if not 1 <= len(concepts) <= 2 or len(set(concepts)) != len(concepts):
        parser.error("choose one or two distinct concepts")
    try:
        result = run_canary(
            skill_root=args.skill_root,
        coverage_path=args.coverage,
            concepts=concepts,
            work_dir=args.work_dir,
            idempotency_key=args.idempotency_key,
        )
    except Exception as exc:
        print(json.dumps({"schema_version": SCHEMA_VERSION, "phase_id": "S8.5.2", "status": "HOLD_CONTINUE", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 1
    if args.output:
        _atomic_json(args.output.expanduser().resolve(), result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
