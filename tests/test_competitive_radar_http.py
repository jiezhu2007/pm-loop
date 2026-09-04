from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from pm_loop_control_plane_server import ControlPlane, ControlPlaneHTTPServer  # noqa: E402
from pm_system_store import PMSystemStore  # noqa: E402
from http_test_utils import create_loopback_server  # noqa: E402


class CompetitiveRadarHTTPTests(unittest.TestCase):
    def test_read_model_and_latest_alias_are_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            html = project / "docs" / "产品情报监控" / "竞品雷达" / "周报" / "report.html"
            markdown = html.with_suffix(".md")
            html.parent.mkdir(parents=True)
            html.write_text("<h1>radar</h1>", encoding="utf-8")
            markdown.write_text("# radar\n", encoding="utf-8")
            state_dir = root / "state"
            store = PMSystemStore(state_dir / "state" / "pm-system.db")
            store.upsert_competitive_radar_latest({"run_id": "r-http", "report_uri": str(markdown), "html_uri": str(html), "report_hash": "sha256:http", "report_status": "reviewed", "gate_status": "PASS", "review_run_id": "review:http", "evidence_coverage": 1.0, "published_at": "2026-09-02T02:00:00Z"})
            controller = ControlPlane(state_dir, ROOT / "scripts" / "pm_loop_control_plane.py", project, root / "codex", ROOT / "web" / "pm-loop-control-plane")
            server = create_loopback_server(ControlPlaneHTTPServer, ("127.0.0.1", 0), controller)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                with urlopen(base + "/api/control-plane/v4/competitive-radar") as response:
                    value = json.loads(response.read().decode("utf-8"))
                    self.assertEqual(value["latest"]["run_id"], "r-http")
                    self.assertEqual(response.headers["ETag"], '"sha256:http"')
                with urlopen(base + "/reports/competitive/latest") as response:
                    self.assertIn("radar", response.read().decode("utf-8"))
                for method in ("/api/control-plane/v4/competitive-radar",):
                    request = __import__("urllib.request", fromlist=["Request"]).Request(base + method, method="POST", data=b"{}", headers={"Content-Type": "application/json"})
                    with self.assertRaises(HTTPError) as raised:
                        urlopen(request)
                    self.assertEqual(raised.exception.code, 405)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_latest_alias_rejects_pointer_outside_report_root_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            report_root = project / "docs" / "产品情报监控" / "竞品雷达" / "周报"
            report_root.mkdir(parents=True)
            secret = root / "not-a-report.html"
            secret.write_text("outside", encoding="utf-8")
            state_dir = root / "state"
            store = PMSystemStore(state_dir / "state" / "pm-system.db")
            controller = ControlPlane(
                state_dir,
                ROOT / "scripts" / "pm_loop_control_plane.py",
                project,
                root / "codex",
                ROOT / "web" / "pm-loop-control-plane",
            )
            server = create_loopback_server(ControlPlaneHTTPServer, ("127.0.0.1", 0), controller)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}"
            try:
                for candidate in (secret, report_root / "linked.html"):
                    if candidate.name == "linked.html":
                        candidate.symlink_to(secret)
                    store.upsert_competitive_radar_latest(
                        {
                            "run_id": "r-rejected",
                            "report_uri": str(candidate.with_suffix(".md")),
                            "html_uri": str(candidate),
                            "report_hash": "sha256:rejected",
                            "report_status": "reviewed",
                            "gate_status": "PASS",
                            "review_run_id": "review:rejected",
                            "evidence_coverage": 1.0,
                            "published_at": "2026-09-04T02:00:00Z",
                        }
                    )
                    with self.assertRaises(HTTPError) as raised:
                        urlopen(base + "/reports/competitive/latest")
                    self.assertEqual(raised.exception.code, 404)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)
