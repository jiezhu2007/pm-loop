from __future__ import annotations

from pathlib import Path

from scripts.artifact_inventory import scan_project, write_outputs
from scripts.artifact_registry_read_model import ArtifactRegistryReadModel


def test_registry_lists_only_policy_eligible_artifacts_and_never_opens_symlink(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    report = docs / "report.html"
    report.write_text("<h1>report</h1>", encoding="utf-8")
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "private.md").write_text("do not index body", encoding="utf-8")
    (tmp_path / ".mcp.json").write_text('{"credential":"private"}', encoding="utf-8")
    inventory_root = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    write_outputs(scan_project(tmp_path, inventory_root, {}), inventory_root)

    model = ArtifactRegistryReadModel(project_root=tmp_path)
    listed = model.list_artifacts(limit=20)
    assert listed["total"] == 1
    item = listed["items"][0]
    assert item["relative_path"] == "docs/report.html"
    assert item["open_kind"] == "html"
    assert model.open_path(item["artifact_id"], "html") == report

    report.unlink()
    report.symlink_to(tmp_path / "memory" / "private.md")
    assert model.open_path(item["artifact_id"], "html") is None

    snapshot = model._load_snapshot()
    assert snapshot is not None
    private = next(row for row in snapshot["root_inventory"] if row["path"] == "memory/private.md")
    credential = next(row for row in snapshot["root_inventory"] if row["path"] == ".mcp.json")
    assert private["artifact_domain"] == "sensitive_metadata"
    assert credential["content_hash"] is None
    assert "memory/private.md" not in [row["relative_path"] for row in snapshot["artifacts"]]


def test_registry_uses_pagination_and_metadata_only_detail(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    for name in ("a.md", "b.md", "c.md"):
        (docs / name).write_text(name, encoding="utf-8")
    inventory_root = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    write_outputs(scan_project(tmp_path, inventory_root, {}), inventory_root)
    model = ArtifactRegistryReadModel(project_root=tmp_path)

    first = model.list_artifacts(limit=2)
    assert first["total"] == 3
    assert first["next_cursor"] == 2
    second = model.list_artifacts(cursor=first["next_cursor"], limit=2)
    assert len(second["items"]) == 1
    detail = model.detail(first["items"][0]["artifact_id"])["artifact"]
    assert "source_path" not in detail
    assert detail["generated_at_status"] == "not_recorded"


def test_legacy_same_stem_representations_are_grouped_and_opened_separately(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    markdown = docs / "weekly.md"
    html = docs / "weekly.html"
    markdown.write_text("# weekly", encoding="utf-8")
    html.write_text("<h1>weekly</h1>", encoding="utf-8")
    inventory_root = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    write_outputs(scan_project(tmp_path, inventory_root, {}), inventory_root)

    model = ArtifactRegistryReadModel(project_root=tmp_path)
    listed = model.list_artifacts(limit=10)
    assert listed["total"] == 1
    item = listed["items"][0]
    assert item["source_kind"] == "legacy_inventory_inferred_representation_group"
    assert {entry["kind"] for entry in item["open_representations"]} == {"html", "markdown"}
    assert model.open_path(item["artifact_id"], "html") == html
    assert model.open_path(item["artifact_id"], "markdown") == markdown


def test_registry_filters_and_detail_keep_unknown_metadata_explicit(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "weekly.md").write_text("# weekly", encoding="utf-8")
    inventory_root = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    write_outputs(scan_project(tmp_path, inventory_root, {}), inventory_root)
    model = ArtifactRegistryReadModel(project_root=tmp_path)

    listed = model.list_artifacts(artifact_domain="business", source_kind="legacy_inventory", time_scope="undated")
    assert listed["total"] == 1
    item = listed["items"][0]
    assert item["generated_at_status"] == "not_recorded"
    assert item["attention_state"] == "needs_evidence"
    detail = model.detail(item["artifact_id"])
    assert detail["artifact"]["related"]["customers"] == []
    assert detail["codex_advice"]["available"] is True
    facets = model.facets()
    assert facets["artifact_domains"] == [{"value": "business", "count": 1}]


def test_registry_prefers_human_readable_artifacts_before_raw_support_files(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "weekly.md").write_text("# weekly", encoding="utf-8")
    (docs / "evidence.json").write_text('{"status":"observed"}', encoding="utf-8")
    inventory_root = tmp_path / "state" / "pm-loop" / "artifact-inventory"
    write_outputs(scan_project(tmp_path, inventory_root, {}), inventory_root)

    listed = ArtifactRegistryReadModel(project_root=tmp_path).list_artifacts(limit=10)
    assert [item["artifact_type"] for item in listed["items"]] == ["markdown", "json"]
