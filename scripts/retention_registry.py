#!/usr/bin/env python3
"""Validated configuration boundary for PM Loop retention.

Configuration can describe known sources and policies.  It cannot introduce
code, commands, roots, SQL, deletion actions, or production authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import fnmatch
import unicodedata
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


SOURCE_SCHEMA = "pm-loop.retention-source-registry.v1"
POLICY_SCHEMA = "pm-loop.retention-policy.v3"
CAPABILITY_SCHEMA = "pm-loop.retention-deletion-capabilities.v1"
def _config_path(name: str) -> Path:
    canonical = Path(__file__).with_name(name)
    return canonical if canonical.is_file() else Path(__file__).resolve().parents[1] / "config" / name


DEFAULT_SOURCE_REGISTRY = _config_path("retention-source-registry.json")
DEFAULT_POLICY = _config_path("retention-policy.v3.json")
DEFAULT_CAPABILITIES = _config_path("retention-deletion-capabilities.json")
RESOLVER_VERSION = "pm-loop.retention-root-resolver.v1"
ADAPTER_BUNDLE_VERSION = "pm-loop.retention-adapters.v1"

ALLOWED_ADAPTERS = frozenset({"filesystem_tree", "compressed_pair", "json_ledger", "sqlite_view", "openviking_api"})
ALLOWED_MODES = frozenset({"observe_only", "disabled"})
ALLOWED_AUTHORITIES = frozenset({"business_record", "decision_record", "audit_evidence", "product_state", "rebuildable_cache"})
ALLOWED_CLASSES = frozenset({"R0", "R1", "R2", "R3", "R4", "R5"})
ALLOWED_ACTIONS = frozenset({"protect", "hold", "repack", "expire"})
CONTRACT_ADAPTERS = {
    "formal-report.v1": frozenset({"filesystem_tree"}),
    "scheduled-state.v1": frozenset({"json_ledger"}),
    "pm-run-artifact.v1": frozenset({"filesystem_tree"}),
    "rebuildable-runtime-backup.v1": frozenset({"filesystem_tree"}),
    "product-intelligence-state.v1": frozenset({"json_ledger"}),
    "pm-timeline.v1": frozenset({"json_ledger"}),
    "concept-state.v1": frozenset({"filesystem_tree"}),
    "operational-log.v1": frozenset({"filesystem_tree"}),
}
REFERENCE_PROVIDERS = frozenset({
    "report-index", "scheduler-run-index", "incident-index", "runtime-current-index",
    "product-intelligence-baseline", "timeline-index", "concept-baseline", "concept-generation-index",
    "runtime-snapshot-index", "log-retention-index",
})
ACTION_PROFILES = {
    "expire-file-v1": {"adapter": "filesystem_tree", "action": "expire", "version": 1},
    "expire-runtime-snapshot-v1": {"adapter": "filesystem_tree", "action": "expire", "version": 1},
    "repack-compressed-pair-v1": {"adapter": "compressed_pair", "action": "repack", "version": 1},
}
DISCOVERY_BASES = {
    "project": ("docs/产品缺口周报", "docs/04-产品设计/资料缺失周报", "state"),
    "pm_loop": ("runs", "state", "scheduler-migration/runtime-backups"),
    "skills": ("product-intelligence-monitor/state", "pm-timeline/state", "shengsuan-concepts/state"),
    "openviking_local": (),
}
_FORBIDDEN_SOURCE_KEYS = frozenset({"command", "commands", "module", "class", "shell", "sql", "script", "script_path", "executable", "endpoint", "url"})


class RetentionConfigError(ValueError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RetentionConfigError(f"cannot read retention config {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RetentionConfigError(f"retention config must be an object: {path}")
    return value


def _text(value: Any, name: str, *, maximum: int = 160) -> str:
    result = str(value or "").strip()
    if not result or len(result) > maximum or any(ord(char) < 32 for char in result):
        raise RetentionConfigError(f"invalid {name}")
    return result


def normalize_relative_path(value: Any) -> str:
    raw = unicodedata.normalize("NFC", _text(value, "relative_path", maximum=500)).replace("\\", "/")
    if "\x00" in raw or raw.startswith("/") or raw.startswith("~"):
        raise RetentionConfigError("relative_path must be a non-empty relative path")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise RetentionConfigError("relative_path contains an unsafe component")
    normalized = str(path)
    if normalized != raw:
        raise RetentionConfigError("relative_path must already be normalized")
    return normalized


def trusted_roots(*, project_root: Optional[Path] = None, home: Optional[Path] = None) -> Dict[str, Path]:
    host_home = Path(home or Path.home()).expanduser().resolve()
    project = Path(project_root or host_home / "Documents" / "project").expanduser().resolve()
    return {
        "project": project,
        "pm_loop": host_home / ".codex" / "pm-loop",
        "skills": host_home / ".codex" / "skills",
        "openviking_local": host_home / ".openviking",
    }


def _is_under(path: str, base: str) -> bool:
    value, parent = PurePosixPath(path), PurePosixPath(base)
    return value == parent or parent in value.parents


def _valid_ignore_pattern(value: Any, name: str) -> str:
    """Validate a source-relative ignore glob without treating it as a path."""
    pattern = _text(value, name, maximum=300).replace("\\", "/")
    if pattern.startswith("/") or pattern.startswith("~") or "\x00" in pattern:
        raise RetentionConfigError(f"invalid {name}")
    if any(part in {"", ".", ".."} for part in PurePosixPath(pattern).parts):
        raise RetentionConfigError(f"invalid {name}")
    return pattern


def _ignore_covers_child(parent: Mapping[str, Any], child_relative: str) -> bool:
    """Allow nested sources only when the parent explicitly excludes them."""
    patterns = parent.get("discovery", {}).get("ignore_relative_paths", [])
    for pattern in patterns:
        if fnmatch.fnmatchcase(child_relative, pattern):
            return True
        if pattern.endswith("/**") and child_relative == pattern[:-3]:
            return True
    return False


def _reject_forbidden_keys(value: Any, *, trail: str = "source") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            if normalized in _FORBIDDEN_SOURCE_KEYS:
                raise RetentionConfigError(f"forbidden executable field {trail}.{key}")
            _reject_forbidden_keys(child, trail=f"{trail}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_forbidden_keys(child, trail=f"{trail}[{index}]")


def _validate_source_registry(value: Mapping[str, Any]) -> Tuple[Dict[str, Any], ...]:
    if value.get("schema_version") != SOURCE_SCHEMA or int(value.get("registry_version") or 0) != 1:
        raise RetentionConfigError("unsupported retention source registry schema/version")
    if value.get("default_mode") != "observe_only":
        raise RetentionConfigError("new retention sources must default to observe_only")
    raw_sources = value.get("sources")
    if not isinstance(raw_sources, list):
        raise RetentionConfigError("sources must be an array")
    ids, paths, result = set(), [], []
    for raw in raw_sources:
        if not isinstance(raw, Mapping):
            raise RetentionConfigError("source entry must be an object")
        _reject_forbidden_keys(raw)
        source_id = _text(raw.get("source_id"), "source_id", maximum=80)
        if not all(char.islower() or char.isdigit() or char == "-" for char in source_id):
            raise RetentionConfigError(f"invalid source_id: {source_id}")
        if source_id in ids:
            raise RetentionConfigError(f"duplicate source_id: {source_id}")
        ids.add(source_id)
        root_ref = raw.get("root_ref")
        if not isinstance(root_ref, Mapping):
            raise RetentionConfigError(f"{source_id}.root_ref must be an object")
        root_id = _text(root_ref.get("root_id"), f"{source_id}.root_id", maximum=40)
        if root_id not in DISCOVERY_BASES:
            raise RetentionConfigError(f"untrusted root_id: {root_id}")
        relative = normalize_relative_path(root_ref.get("relative_path"))
        if not any(_is_under(relative, base) for base in DISCOVERY_BASES[root_id]):
            raise RetentionConfigError(f"source path is outside fixed discovery bases: {source_id}")
        adapter = _text(raw.get("adapter"), f"{source_id}.adapter", maximum=40)
        contract = _text(raw.get("object_contract"), f"{source_id}.object_contract", maximum=100)
        if adapter not in ALLOWED_ADAPTERS or adapter not in CONTRACT_ADAPTERS.get(contract, frozenset()):
            raise RetentionConfigError(f"unsupported adapter/contract: {source_id}")
        authority = _text(raw.get("declared_authority"), f"{source_id}.declared_authority", maximum=60)
        if authority not in ALLOWED_AUTHORITIES:
            raise RetentionConfigError(f"unsupported authority: {source_id}")
        mode = _text(raw.get("mode"), f"{source_id}.mode", maximum=30)
        if mode not in ALLOWED_MODES:
            raise RetentionConfigError(f"source mode cannot grant production deletion: {source_id}")
        discovery = raw.get("discovery")
        if not isinstance(discovery, Mapping) or int(discovery.get("max_depth") or 0) not in range(1, 7):
            raise RetentionConfigError(f"invalid discovery contract: {source_id}")
        for field in ("include_names", "exclude_names"):
            names = discovery.get(field)
            if not isinstance(names, list) or not all(isinstance(item, str) and item and len(item) <= 100 for item in names):
                raise RetentionConfigError(f"invalid {source_id}.{field}")
        ignored_paths = discovery.get("ignore_relative_paths", [])
        if not isinstance(ignored_paths, list):
            raise RetentionConfigError(f"invalid {source_id}.ignore_relative_paths")
        for index, pattern in enumerate(ignored_paths):
            _valid_ignore_pattern(pattern, f"{source_id}.ignore_relative_paths[{index}]")
        providers = raw.get("reference_providers")
        if not isinstance(providers, list) or any(item not in REFERENCE_PROVIDERS for item in providers):
            raise RetentionConfigError(f"invalid reference provider: {source_id}")
        sla = int(raw.get("freshness_sla_hours") or 0)
        if sla < 1 or sla > 24 * 365:
            raise RetentionConfigError(f"invalid freshness SLA: {source_id}")
        pair = (root_id, relative, source_id)
        for prior_root, prior_relative, prior_id in paths:
            if root_id != prior_root:
                continue
            if _is_under(relative, prior_relative):
                nested = str(PurePosixPath(relative).relative_to(PurePosixPath(prior_relative)))
                prior = next(item for item in result if item["source_id"] == prior_id)
                if not _ignore_covers_child(prior, nested):
                    raise RetentionConfigError(f"overlapping source subtrees: {prior_id}, {source_id}")
            elif _is_under(prior_relative, relative):
                nested = str(PurePosixPath(prior_relative).relative_to(PurePosixPath(relative)))
                if not _ignore_covers_child(raw, nested):
                    raise RetentionConfigError(f"overlapping source subtrees: {prior_id}, {source_id}")
        paths.append(pair)
        result.append(dict(raw))
    return tuple(result)


def _validate_policy(value: Mapping[str, Any]) -> Tuple[Dict[str, Any], ...]:
    if value.get("schema_version") != POLICY_SCHEMA or int(value.get("policy_version") or 0) != 3:
        raise RetentionConfigError("unsupported retention policy schema/version")
    if value.get("default_class") != "R5" or value.get("default_action") != "hold":
        raise RetentionConfigError("retention policy must fail closed to R5/hold")
    rules = value.get("rules")
    if not isinstance(rules, list):
        raise RetentionConfigError("retention policy rules must be an array")
    ids, result = set(), []
    allowed_match = {"source_id", "adapter", "object_contract", "authority", "equivalence", "reference_state", "terminal_status", "latest_state"}
    for raw in rules:
        if not isinstance(raw, Mapping):
            raise RetentionConfigError("policy rule must be an object")
        rule_id = _text(raw.get("rule_id"), "rule_id", maximum=100)
        if rule_id in ids:
            raise RetentionConfigError(f"duplicate policy rule: {rule_id}")
        ids.add(rule_id)
        match = raw.get("match")
        if not isinstance(match, Mapping) or not match or any(key not in allowed_match for key in match):
            raise RetentionConfigError(f"unsupported policy matcher: {rule_id}")
        if any(not isinstance(values, list) or not values or not all(isinstance(item, str) for item in values) for values in match.values()):
            raise RetentionConfigError(f"policy matcher values must be string arrays: {rule_id}")
        if raw.get("class") not in ALLOWED_CLASSES or raw.get("action") not in ALLOWED_ACTIONS:
            raise RetentionConfigError(f"invalid policy result: {rule_id}")
        if raw.get("mode") != "observe_only":
            raise RetentionConfigError(f"policy rule cannot grant production deletion: {rule_id}")
        for field in ("hot_days", "quarantine_days"):
            if field in raw and int(raw[field]) not in range(0, 3651):
                raise RetentionConfigError(f"invalid {field}: {rule_id}")
        result.append(dict(raw))
    return tuple(result)


def _parse_timestamp(value: Any, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value, name, maximum=40).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RetentionConfigError(f"invalid {name}") from exc
    if parsed.tzinfo is None:
        raise RetentionConfigError(f"{name} must include timezone")
    return parsed.astimezone(timezone.utc)


def _validate_capabilities(value: Mapping[str, Any], sources: Tuple[Dict[str, Any], ...]) -> Tuple[Dict[str, Any], ...]:
    if value.get("schema_version") != CAPABILITY_SCHEMA or int(value.get("capability_version") or 0) != 1:
        raise RetentionConfigError("unsupported retention capability schema/version")
    if not isinstance(value.get("kill_switch"), bool) or value.get("default_action") != "deny":
        raise RetentionConfigError("capabilities must provide an explicit kill switch and deny default")
    capabilities = value.get("capabilities")
    if not isinstance(capabilities, list):
        raise RetentionConfigError("capabilities must be an array")
    required = {"capability_id", "source_id", "root_id", "relative_subtree", "object_contract", "action_profile", "rollout_phase", "max_objects_per_batch", "max_bytes_per_day", "valid_from", "expires_at", "approving_adr", "signature"}
    source_by_id = {str(item["source_id"]): item for item in sources}
    seen = set()
    for raw in capabilities:
        if not isinstance(raw, Mapping) or not required.issubset(raw):
            raise RetentionConfigError("capability is missing its exact authorization boundary")
        capability_id = _text(raw.get("capability_id"), "capability_id", maximum=100)
        if capability_id in seen:
            raise RetentionConfigError("duplicate retention capability")
        seen.add(capability_id)
        source = source_by_id.get(str(raw.get("source_id") or ""))
        if source is None:
            raise RetentionConfigError("capability references unknown source")
        profile = ACTION_PROFILES.get(str(raw.get("action_profile") or ""))
        if profile is None:
            raise RetentionConfigError("unknown fixed action profile")
        subtree = normalize_relative_path(raw.get("relative_subtree"))
        source_root = source["root_ref"]
        if raw.get("root_id") != source_root.get("root_id") or not _is_under(subtree, str(source_root.get("relative_path") or "")):
            raise RetentionConfigError("capability expands beyond registered source subtree")
        if raw.get("object_contract") != source.get("object_contract") or profile["adapter"] != source.get("adapter"):
            raise RetentionConfigError("capability contract/action profile mismatch")
        phase = str(raw.get("rollout_phase") or "")
        if phase not in {"canary", "cohort", "stable"}:
            raise RetentionConfigError("invalid retention rollout phase")
        max_objects = int(raw.get("max_objects_per_batch") or 0)
        max_bytes = int(raw.get("max_bytes_per_day") or 0)
        if max_objects < 1 or max_objects > 1000 or max_bytes < 1:
            raise RetentionConfigError("invalid retention capability quota")
        if phase == "canary" and max_objects != 1:
            raise RetentionConfigError("canary capability must be limited to one object")
        valid_from = _parse_timestamp(raw.get("valid_from"), "valid_from")
        expires_at = _parse_timestamp(raw.get("expires_at"), "expires_at")
        if expires_at <= valid_from:
            raise RetentionConfigError("retention capability validity window is empty")
        if not re.fullmatch(r"ADR-[0-9]{3,}", str(raw.get("approving_adr") or "")):
            raise RetentionConfigError("retention capability requires an approving ADR")
        if not str(raw.get("signature") or "").startswith("base64:"):
            raise RetentionConfigError("retention capability signature is invalid")
    return tuple(dict(item) for item in capabilities)


def verify_capability_signature(capability: Mapping[str, Any], signing_key: bytes, *, current: Optional[datetime] = None) -> bool:
    """Validate one time-bounded capability without trusting registry fields as authority."""
    if not signing_key:
        return False
    now = (current or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        if not (_parse_timestamp(capability.get("valid_from"), "valid_from") <= now <= _parse_timestamp(capability.get("expires_at"), "expires_at")):
            return False
    except RetentionConfigError:
        return False
    unsigned = dict(capability)
    actual = str(unsigned.pop("signature", ""))
    digest = hmac.new(signing_key, canonical_json(unsigned).encode("utf-8"), hashlib.sha256).digest()
    import base64

    expected = "base64:" + base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(actual, expected)


@dataclass(frozen=True)
class RetentionBundle:
    registry: Dict[str, Any]
    policy: Dict[str, Any]
    capabilities: Dict[str, Any]
    sources: Tuple[Dict[str, Any], ...]
    rules: Tuple[Dict[str, Any], ...]
    capability_rows: Tuple[Dict[str, Any], ...]
    source_registry_hash: str
    policy_hash: str
    deletion_capability_hash: str

    @property
    def global_mode(self) -> str:
        if self.capabilities.get("kill_switch", True):
            return "disabled"
        return "observe_only" if not self.capability_rows else "enabled"


def load_bundle(
    registry_path: Path = DEFAULT_SOURCE_REGISTRY,
    policy_path: Path = DEFAULT_POLICY,
    capabilities_path: Path = DEFAULT_CAPABILITIES,
) -> RetentionBundle:
    registry, policy, capabilities = _read_json(registry_path), _read_json(policy_path), _read_json(capabilities_path)
    sources = _validate_source_registry(registry)
    rules = _validate_policy(policy)
    capability_rows = _validate_capabilities(capabilities, sources)
    return RetentionBundle(
        registry=registry,
        policy=policy,
        capabilities=capabilities,
        sources=sources,
        rules=rules,
        capability_rows=capability_rows,
        source_registry_hash=canonical_hash(registry),
        policy_hash=canonical_hash(policy),
        deletion_capability_hash=canonical_hash(capabilities),
    )


def resolve_source_path(source: Mapping[str, Any], roots: Mapping[str, Path]) -> Path:
    root_ref = source["root_ref"]
    root_id = str(root_ref["root_id"])
    relative = normalize_relative_path(root_ref["relative_path"])
    root = Path(roots[root_id]).expanduser().resolve()
    candidate = root.joinpath(*PurePosixPath(relative).parts)
    current = root
    root_stat = root.stat() if root.exists() else None
    for part in PurePosixPath(relative).parts:
        current = current / part
        if current.exists() or current.is_symlink():
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise RetentionConfigError(f"symlink is not allowed in source path: {source['source_id']}")
            if root_stat is not None and info.st_dev != root_stat.st_dev:
                raise RetentionConfigError(f"mount crossing is not allowed: {source['source_id']}")
    resolved_parent = candidate.parent.resolve(strict=False)
    try:
        resolved_parent.relative_to(root)
    except ValueError as exc:
        raise RetentionConfigError(f"source path escapes trusted root: {source['source_id']}") from exc
    return candidate


def policy_for(item: Mapping[str, Any], bundle: RetentionBundle) -> Dict[str, Any]:
    for rule in bundle.rules:
        match = rule["match"]
        if all(str(item.get(field) or "") in values for field, values in match.items()):
            return dict(rule)
    return {"rule_id": "default-unclassified", "class": "R5", "action": "hold", "mode": "observe_only"}


def matching_capability(
    item: Mapping[str, Any],
    action_profile: str,
    bundle: RetentionBundle,
    *,
    signing_key: Optional[bytes] = None,
    current: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    if bundle.capabilities.get("kill_switch", True):
        return None
    for capability in bundle.capability_rows:
        if capability.get("source_id") != item.get("source_id") or capability.get("root_id") != item.get("root_id"):
            continue
        if capability.get("object_contract") != item.get("object_contract") or capability.get("action_profile") != action_profile:
            continue
        if _is_under(str(item.get("relative_path") or ""), str(capability.get("relative_subtree") or "")) and verify_capability_signature(capability, signing_key or b"", current=current):
            return capability
    return None


def root_identities(roots: Mapping[str, Path]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for root_id, root in roots.items():
        path = Path(root)
        if path.exists():
            info = path.stat()
            result[root_id] = {"available": True, "st_dev": info.st_dev, "st_ino": info.st_ino, "identity_hash": canonical_hash([root_id, info.st_dev, info.st_ino])}
        else:
            result[root_id] = {"available": False, "st_dev": None, "st_ino": None, "identity_hash": canonical_hash([root_id, "missing"])}
    return result


def worker_build_digest(paths: Iterable[Path]) -> str:
    rows = []
    for path in paths:
        value = Path(path)
        rows.append([value.name, file_hash(value) if value.is_file() else "missing"])
    return canonical_hash(rows)


__all__ = [
    "ACTION_PROFILES", "ADAPTER_BUNDLE_VERSION", "CAPABILITY_SCHEMA", "DEFAULT_CAPABILITIES",
    "DEFAULT_POLICY", "DEFAULT_SOURCE_REGISTRY", "DISCOVERY_BASES", "POLICY_SCHEMA", "RESOLVER_VERSION",
    "RetentionBundle", "RetentionConfigError", "SOURCE_SCHEMA", "canonical_hash", "canonical_json", "file_hash",
    "load_bundle", "matching_capability", "normalize_relative_path", "policy_for", "resolve_source_path",
    "root_identities", "trusted_roots", "verify_capability_signature", "worker_build_digest",
]
