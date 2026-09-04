#!/usr/bin/env python3
"""Deterministic local DeepSeek-compatible stream for DSH integration tests.

The server never fabricates PM facts. It asks the real DSH agent to call
``pm_loop_snapshot`` once, then returns a short final acknowledgement after
the real tool result appears in the next request.
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List


def sse_chunk(request_id: str, model: str, delta: Dict[str, Any], finish_reason: Any = None) -> str:
    payload = {
        "id": request_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


class MockDeepSeekHandler(BaseHTTPRequestHandler):
    server_version = "PMLoopMockDeepSeek/1.0"

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        length = int(self.headers.get("content-length", "0"))
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.send_error(400, "invalid JSON")
            return

        messages = body.get("messages") if isinstance(body, dict) else []
        messages = messages if isinstance(messages, list) else []
        tools = body.get("tools") if isinstance(body, dict) else []
        tools = tools if isinstance(tools, list) else []
        tool_names = [
            item.get("function", {}).get("name")
            for item in tools
            if isinstance(item, dict) and isinstance(item.get("function"), dict)
        ]
        has_pm_tool = "pm_loop_snapshot" in tool_names
        has_tool_result = any(isinstance(item, dict) and item.get("role") == "tool" for item in messages)
        response_mode = "tool_call" if has_pm_tool and not has_tool_result else "final"
        request_summary = {
            "ts": time.time(),
            "path": self.path,
            "request_index": self.server.request_count + 1,
            "message_roles": [item.get("role") for item in messages if isinstance(item, dict)],
            "tool_names": tool_names,
            "has_pm_loop_tool": has_pm_tool,
            "has_tool_result": has_tool_result,
            "response_mode": response_mode,
        }
        self.server.request_count += 1
        self.server.request_summaries.append(request_summary)
        if self.server.log_path is not None:
            with self.server.log_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(request_summary, ensure_ascii=False) + "\n")

        model = str(body.get("model", "deepseek-v4-flash")) if isinstance(body, dict) else "deepseek-v4-flash"
        request_id = f"pm-loop-mock-{self.server.request_count:03d}"
        if response_mode == "tool_call":
            stream = "".join(
                [
                    sse_chunk(
                        request_id,
                        model,
                        {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_pm_loop_snapshot_001",
                                    "type": "function",
                                    "function": {"name": "pm_loop_snapshot", "arguments": '{"record":false}'},
                                }
                            ],
                        },
                    ),
                    sse_chunk(request_id, model, {}, "tool_calls"),
                    "data: [DONE]\n\n",
                ]
            )
        else:
            stream = "".join(
                [
                    sse_chunk(
                        request_id,
                        model,
                        {"role": "assistant", "content": "已通过 pm_loop_snapshot 读取本地 PM Loop 快照。"},
                    ),
                    sse_chunk(request_id, model, {}, "stop"),
                    "data: [DONE]\n\n",
                ]
            )

        encoded = stream.encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "close")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)
        self.wfile.flush()

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class MockDeepSeekServer(ThreadingHTTPServer):
    request_count: int
    request_summaries: List[Dict[str, Any]]
    log_path: Path | None

    def __init__(self, address: tuple[str, int], log_path: Path | None) -> None:
        super().__init__(address, MockDeepSeekHandler)
        self.request_count = 0
        self.request_summaries = []
        self.log_path = log_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local deterministic DeepSeek SSE endpoint for PM Loop DSH tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--log", type=Path, required=True, help="write bounded request summaries as JSONL")
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    server = MockDeepSeekServer((args.host, args.port), args.log)
    print(json.dumps({"status": "ready", "url": f"http://{args.host}:{args.port}", "log": str(args.log)}, ensure_ascii=False), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
