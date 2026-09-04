from __future__ import annotations

import json
import hashlib
from pathlib import Path

from scripts.artifact_manifest import INDEX_SCHEMA, SCHEMA, write_worker_artifact_manifest
from scripts.artifact_registry_read_model import ArtifactRegistryReadModel


def _package(root: Path, content: str = "报告正文", *, html: bool = False) -> dict:
    report = root / "docs" / ("报告.html" if html else "报告.md")
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(content, encoding="utf-8")
    return {
        "task": {"schedule_key": "databuilder-product-gap-report"},
        "execution": {"occurrence_id": "occ-1", "job_id": "job-1", "run_id": "run-1"},
        "outcome": {"execution_status": "completed"},
        "stages": [{"completed_at": "2026-09-04T08:00:00Z"}],
        "artifacts": [
            {
                "role": "primary_html" if html else "primary_markdown",
                "uri": str(report),
                "sha256": "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest(),
            },
            {"role": "handler_output", "uri": "/private/outside.log", "sha256": "sha256:outside"},
        ],
    }


def test_worker_manifest_is_private_idempotent_and_only_exposes_project_representations(tmp_path: Path) -> None:
    package = _package(tmp_path)
    path = write_worker_artifact_manifest(project_root=tmp_path, package=package)
    value = json.loads(path.read_text(encoding="utf-8"))
    assert value["schema_version"] == SCHEMA
    assert value["visibility"] == "local_private"
    assert value["representations"] == {"markdown_path": "docs/报告.md"}
    assert value["evidence"]["artifact_count"] == 2
    index = json.loads((tmp_path / "state/pm-loop/artifact-registry/manifest-index.json").read_text(encoding="utf-8"))
    assert index["schema_version"] == INDEX_SCHEMA
    assert len(index["entries"]) == 1
    assert "/private/outside.log" not in path.read_text(encoding="utf-8")
    assert write_worker_artifact_manifest(project_root=tmp_path, package=package) == path


def test_manifest_is_visible_without_inventory_rescan_and_preserves_representation_links(tmp_path: Path) -> None:
    package = _package(tmp_path, html=True)
    manifest_path = write_worker_artifact_manifest(project_root=tmp_path, package=package)
    model = ArtifactRegistryReadModel(project_root=tmp_path)
    listed = model.list_artifacts(limit=10)
    assert listed["total"] == 1
    item = listed["items"][0]
    assert item["source_kind"] == "worker_manifest"
    assert item["open_representations"][0]["kind"] == "html"
    assert model.open_path(item["artifact_id"], "html") == tmp_path / "docs/报告.html"
    assert manifest_path.is_file()


def test_changed_content_for_same_occurrence_links_supersedes(tmp_path: Path) -> None:
    first = _package(tmp_path, "v1")
    first_path = write_worker_artifact_manifest(project_root=tmp_path, package=first)
    second = _package(tmp_path, "v2")
    second["execution"]["run_id"] = "run-2"
    second_path = write_worker_artifact_manifest(project_root=tmp_path, package=second)
    index = json.loads((tmp_path / "state/pm-loop/artifact-registry/manifest-index.json").read_text(encoding="utf-8"))
    assert len(index["entries"]) == 2
    current = [item for item in index["entries"] if item["artifact_id"] != json.loads(first_path.read_text(encoding="utf-8"))["artifact_id"]][0]
    assert current["supersedes"] == json.loads(first_path.read_text(encoding="utf-8"))["artifact_id"]
    second["execution"]["run_id"] = "run-retry"
    second["stages"][0]["completed_at"] = "2026-09-04T08:05:00Z"
    assert write_worker_artifact_manifest(project_root=tmp_path, package=second) == second_path
    index = json.loads((tmp_path / "state/pm-loop/artifact-registry/manifest-index.json").read_text(encoding="utf-8"))
    current = next(item for item in index["entries"] if item["artifact_id"] == json.loads(second_path.read_text(encoding="utf-8"))["artifact_id"])
    assert [run["run_id"] for run in current["runs"]] == ["run-2", "run-retry"]
