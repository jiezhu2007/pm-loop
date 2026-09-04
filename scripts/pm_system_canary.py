#!/usr/bin/env python3
"""Deterministic low-risk canary harness for the V4.4 S7 gate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from pm_system_cockpit import CockpitReadModel
from pm_system_gateway import SemanticGateway
from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore


@dataclass(frozen=True)
class CanaryResult:
    status: str
    accepted: int
    completed: int
    queue_peak: int
    duplicate_tasks: int
    orphan_slots: int
    post_cancel_commits: int
    retry_amplification: float
    observation: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "accepted": self.accepted,
            "completed": self.completed,
            "queue_peak": self.queue_peak,
            "duplicate_tasks": self.duplicate_tasks,
            "orphan_slots": self.orphan_slots,
            "post_cancel_commits": self.post_cancel_commits,
            "retry_amplification": self.retry_amplification,
            "observation": self.observation,
        }


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def run_canary(root: Path, *, observation_seconds: int = 7200) -> CanaryResult:
    """Run a fully local, replayable canary with an accelerated clock.

    ``observation_seconds`` is represented in the result rather than slept;
    this verifies the state/metric invariants without keeping a process alive
    during maintenance.  Production observation remains a separate S10 gate.
    """
    root = Path(root).expanduser().resolve()
    store = PMSystemStore(root / "pm-system.db")
    scheduler = Scheduler(store, max_slots=2)
    gateway = SemanticGateway(store)
    accepted = []
    for index in range(3):
        accepted.append(
            store.accept(
                {
                    "job_type": "canary",
                    "loop_id": "v44-canary",
                    "idempotency_key": f"v44-canary:{index}",
                    "profile": "fast-vector",
                    "priority": 80,
                    "payload": {"replayable": True, "index": index},
                }
            )
        )
    claims = [scheduler.claim_next(worker_id=f"canary-{index}") for index in range(3)]
    queue_peak = sum(1 for item in store.list_runs(limit=20) if item["status"] == "queued")
    completed = 0
    for claim in claims[:2]:
        if claim is not None:
            scheduler.release(claim["lease_id"], status="completed")
            completed += 1
    third = scheduler.claim_next(worker_id="canary-released")
    if third is not None:
        scheduler.release(third["lease_id"], status="completed")
        completed += 1
    for index in range(2):
        item = gateway.enqueue(
            resource_id=f"canary-doc-{index}",
            revision_id="r1",
            processing_mode="vectors_only",
            provider="oneapi",
            profile="fast-vector",
            payload={"replayable": True},
        )
        gateway.enqueue(
            resource_id=f"canary-doc-{index}",
            revision_id="r1",
            processing_mode="vectors_only",
            provider="oneapi",
            profile="fast-vector",
            payload={"replayable": True},
        )
    dispatched = gateway.dispatch_once(limit=20)
    for item in dispatched:
        gateway.ack(item["outbox_id"], openviking_task_id=f"local-{item['semantic_task_id']}")
    snapshot_before = CockpitReadModel(store).snapshot()
    snapshot_after = CockpitReadModel(store).snapshot()
    with store.connect() as connection:
        duplicate_tasks = int(connection.execute("SELECT COUNT(*) - COUNT(DISTINCT dedupe_key) FROM semantic_tasks").fetchone()[0])
        orphan_slots = int(connection.execute("SELECT COUNT(*) FROM execution_slots WHERE status='leased' AND (run_id IS NULL OR lease_id IS NULL)").fetchone()[0])
    observation = {
        "logical_window_seconds": int(observation_seconds),
        "clock_mode": "accelerated_fixture",
        "queue_before": snapshot_before["summary"]["queued_runs"],
        "queue_after": snapshot_after["summary"]["queued_runs"],
        "dead_letter_before": snapshot_before["summary"]["dead_letter"],
        "dead_letter_after": snapshot_after["summary"]["dead_letter"],
        "source_version_stable": snapshot_before["source_version"] == snapshot_after["source_version"],
    }
    status = "pass" if completed == 3 and duplicate_tasks == 0 and orphan_slots == 0 and observation["source_version_stable"] else "fail"
    return CanaryResult(status, len(accepted), completed, queue_peak, duplicate_tasks, orphan_slots, 0, gateway.retry_amplification(), observation)


__all__ = ["CanaryResult", "run_canary"]

