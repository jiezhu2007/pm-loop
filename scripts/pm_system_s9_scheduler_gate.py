#!/usr/bin/env python3
"""Run the isolated S9.2.4 admission, slot, and checkpoint gate.

The gate uses temporary SQLite files only.  It never opens the production
coordination database, starts a worker, or calls a model/provider.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from pm_system_scheduler import Scheduler
from pm_system_store import PMSystemStore


def submit(store: PMSystemStore, key: str) -> dict:
    return store.accept({"job_type": "run", "loop_id": "s924-gate", "idempotency_key": key, "profile": "interactive"})


def run_gate() -> dict:
    with tempfile.TemporaryDirectory(prefix="pm-v44-s924-", dir="/private/tmp") as temp:
        root = Path(temp)
        freeze_store = PMSystemStore(root / "freeze.db")
        with patch.dict(os.environ, {"PM_V44_ADMISSION": "freeze", "PM_V44_MAX_CODEX_SLOTS": "0"}, clear=False):
            frozen_scheduler = Scheduler(freeze_store)
            frozen_run = submit(freeze_store, "freeze-1")
            frozen_claim = frozen_scheduler.claim_next(worker_id="freeze")
            frozen_status = freeze_store.get_run(frozen_run["run_id"])

        canary_store = PMSystemStore(root / "canary.db")
        with patch.dict(os.environ, {"PM_V44_ADMISSION": "canary", "PM_V44_MAX_CODEX_SLOTS": "2"}, clear=False):
            scheduler = Scheduler(canary_store)
            runs = [submit(canary_store, f"canary-{index}") for index in range(3)]
            first = scheduler.claim_next(worker_id="canary-1", pid=101, process_group_id=101)
            second = scheduler.claim_next(worker_id="canary-2", pid=102, process_group_id=102)
            third = scheduler.claim_next(worker_id="canary-3")
            third_status_before_reconcile = canary_store.get_run(runs[2]["run_id"])["status"]
            checkpoint_call = scheduler.begin_model_call(
                first["run_id"],
                stage="analysis",
                model_input_hash="s924-input-hash",
                prompt_version="s924-v1",
                provider="oneapi",
            )
            with canary_store.connect() as connection:
                checkpoint_before_finish = connection.execute(
                    "SELECT input_hash, payload_json FROM checkpoints WHERE run_id=? AND stage='analysis' AND checkpoint_key='model_call'",
                    (first["run_id"],),
                ).fetchone()
            checkpoint_created_status = json.loads(checkpoint_before_finish[1])["status"]
            scheduler.finish_model_call(checkpoint_call["call_id"], status="completed", artifact_uri="artifact://s924-result")
            with canary_store.connect() as connection:
                checkpoint_after_finish = connection.execute(
                    "SELECT input_hash, payload_json FROM checkpoints WHERE run_id=? AND stage='analysis' AND checkpoint_key='model_call'",
                    (first["run_id"],),
                ).fetchone()
            reconcile = scheduler.startup_reconcile(active_lease_ids=[])
            resumed = scheduler.claim_next(worker_id="canary-resumed")
            if resumed is not None:
                scheduler.release(resumed["lease_id"], status="completed")
            slots = scheduler.slot_snapshot()
            run_states = {run["run_id"]: canary_store.get_run(run["run_id"])["status"] for run in runs}

        return {
            "schema_version": "pm-system.s9.2.4-scheduler-gate.v1",
            "phase_id": "S9.2.4",
            "isolated": True,
            "freeze": {
                "admission": frozen_scheduler.admission_snapshot(),
                "claim": frozen_claim,
                "run_status": frozen_status["status"],
            },
            "canary": {
                "admission": scheduler.admission_snapshot(),
                "claimed_run_ids": [first["run_id"], second["run_id"]],
                "third_claim": third,
                "queued_third_status": third_status_before_reconcile,
                "checkpoint": {
                    "input_hash": checkpoint_after_finish[0],
                    "created_status": checkpoint_created_status,
                    "payload_status": json.loads(checkpoint_after_finish[1])["status"],
                    "model_call_attempt": checkpoint_call["attempt"],
                },
                "startup_reconcile": reconcile,
                "resumed_after_reconcile": resumed is not None,
                "run_states": run_states,
                "slots_final": slots,
                "orphan_slots": sum(1 for slot in slots if slot["status"] == "leased" and not slot.get("run_id")),
            },
            "external_provider_calls": 0,
            "production_state_touched": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_gate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
