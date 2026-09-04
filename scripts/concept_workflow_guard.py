"""Single switch for the retired Concept Learning write paths."""

from __future__ import annotations

import json
from typing import Any, Dict


CONCEPT_REFRESH_DISABLED = True
DISABLED_REASON = "概念刷新自动链路已停用；Active、Candidate、discovery 和 usage 仅保留历史只读展示"


def disabled_payload(operation: str) -> Dict[str, Any]:
    return {
        "status": "disabled",
        "read_only": True,
        "operation": operation,
        "reason": DISABLED_REASON,
    }


def emit_disabled(operation: str) -> int:
    print(json.dumps(disabled_payload(operation), ensure_ascii=False))
    return 0
