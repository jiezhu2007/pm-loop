import json
from pathlib import Path

from scripts.artifact_inventory import read_manifest, scan_project, write_outputs


def test_inventory_is_hash_idempotent_and_excludes_transient_dirs(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "tmp").mkdir()
    (tmp_path / ".tmp-run").mkdir()
    (tmp_path / "docs" / "report.md").write_text("hello", encoding="utf-8")
    (tmp_path / "tmp" / "ignored.md").write_text("ignored", encoding="utf-8")
    (tmp_path / ".tmp-run" / "ignored.md").write_text("ignored", encoding="utf-8")
    output = tmp_path / "state" / "pm-loop" / "artifact-inventory"

    first = scan_project(tmp_path, output, {})
    second = scan_project(tmp_path, output, first)

    assert first["inventory_hash"] == second["inventory_hash"]
    assert first["summary"]["regular_file_count"] == 1
    assert first["summary"]["root_regular_file_count"] == 3
    assert first["summary"]["excluded_directory_count"] >= 2
    tmp_entry = next(item for item in first["root_inventory"] if item["path"] == "tmp/ignored.md")
    assert tmp_entry["status"] == "excluded"
    assert tmp_entry["exclusion_reason"] == "cache_or_temporary_directory"
    assert second["artifacts"][0]["artifact_id"] == first["artifacts"][0]["artifact_id"]


def test_changed_file_links_supersedes_and_unseen_is_unknown(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    report = tmp_path / "docs" / "report.md"
    report.write_text("v1", encoding="utf-8")
    output = tmp_path / "inventory"
    output.mkdir()
    first = scan_project(tmp_path, output, {})
    first_report = next(item for item in first["artifacts"] if item["relative_path"] == "docs/report.md")
    report.write_text("v2", encoding="utf-8")
    second = scan_project(tmp_path, output, first)
    second_report = next(item for item in second["artifacts"] if item["relative_path"] == "docs/report.md")
    assert second_report["supersedes"] == first_report["artifact_id"]

    report.unlink()
    third = scan_project(tmp_path, output, second)
    assert third["summary"]["previous_unseen_count"] == 1
    assert third["previous_unseen"][0]["conclusion"] == "unknown_not_observed"
    assert third["completeness"]["deletion_conclusion_allowed"] is False


def test_write_outputs_can_be_read_back(tmp_path: Path) -> None:
    output = tmp_path / "inventory"
    value = scan_project(tmp_path, output, {})
    paths = write_outputs(value, output)
    assert all(path.is_file() for path in paths.values())
    assert read_manifest(paths["latest"])["inventory_hash"] == value["inventory_hash"]
    legacy = json.loads(paths["legacy_manifest"].read_text(encoding="utf-8"))
    assert legacy["read_only"] is True
    assert legacy["inventory_snapshot_path"].endswith(".json.gz")
    assert legacy["artifact_registry_hash"].startswith("sha256:")
    repeat = scan_project(tmp_path, output, read_manifest(paths["latest"]))
    assert repeat["inventory_hash"] == value["inventory_hash"]


def test_symlink_is_recorded_but_never_followed(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    target.write_text("private", encoding="utf-8")
    link = tmp_path / "shortcut.txt"
    link.symlink_to(target)
    value = scan_project(tmp_path, tmp_path / "inventory", {})
    shortcut = next(item for item in value["root_inventory"] if item["path"] == "shortcut.txt")
    assert shortcut["kind"] == "symlink"
    assert shortcut["status"] == "excluded"
    assert shortcut["exclusion_reason"] == "symlink_not_followed"
    assert all(item["relative_path"] != "shortcut.txt" for item in value["artifacts"])


def test_registry_hides_known_backup_and_system_metadata_but_keeps_root_inventory(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "report.md").write_text("report", encoding="utf-8")
    (tmp_path / "docs" / ".DS_Store").write_text("metadata", encoding="utf-8")
    (tmp_path / "docs" / ".session-marker.json").write_text("metadata", encoding="utf-8")
    backup = tmp_path / "outputs" / "openviking-skill-conflicts-20260814" / "entry-backups"
    backup.mkdir(parents=True)
    (backup / "stale.md").write_text("backup", encoding="utf-8")
    report = tmp_path / "outputs" / "market-report.md"
    report.parent.mkdir(exist_ok=True)
    report.write_text("current", encoding="utf-8")

    value = scan_project(tmp_path, tmp_path / "inventory", {})
    registry_paths = {item["relative_path"] for item in value["artifacts"]}
    root_paths = {item["path"] for item in value["root_inventory"]}

    assert registry_paths == {"docs/report.md", "outputs/market-report.md"}
    assert "docs/.DS_Store" in root_paths
    assert "outputs/openviking-skill-conflicts-20260814/entry-backups/stale.md" in root_paths


def test_budget_exhaustion_keeps_last_complete_latest_pointer(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "report.md").write_text("complete", encoding="utf-8")
    output = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    complete = scan_project(tmp_path, output, {})
    write_outputs(complete, output)
    latest_before = (output / "latest.json").read_text(encoding="utf-8")

    incomplete = scan_project(tmp_path, output, complete, max_seconds=0)
    paths = write_outputs(incomplete, output)

    assert incomplete["scan_status"] == "partial"
    assert incomplete["completeness"]["inventory_complete"] is False
    assert incomplete["summary"]["budget_exhausted"] is True
    assert (output / "latest.json").read_text(encoding="utf-8") == latest_before
    attempt = read_manifest(paths["last_attempt"])
    assert attempt["completeness"]["inventory_complete"] is False
