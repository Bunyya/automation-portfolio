"""Command-line entry point for the offline demonstration."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .pipeline import InputMessage, NotificationService, SimulatedSink


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline synthetic notification demo")
    parser.add_argument("--input", type=Path, required=True, help="JSON file with synthetic messages")
    parser.add_argument("--state", type=Path, default=Path("demo-state.sqlite3"))
    parser.add_argument("--fail-recipient", choices=["demo-students", "demo-staff", "demo-operators"])
    args = parser.parse_args()

    try:
        raw = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("input must be a JSON array")
        messages = [InputMessage(**item) for item in raw]
        sink = SimulatedSink(args.fail_recipient)
        with NotificationService(args.state) as service:
            results = [service.process(message, sink) for message in messages]
    except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
        print(f"Input or state error: {type(exc).__name__}", file=sys.stderr)
        return 2

    report = {
        "mode": "offline simulation",
        "results": [
            {
                "message_id": result.message_id,
                "kind": result.kind.value,
                "emitted": result.emitted,
                "skipped": result.skipped,
                "failed": result.failed,
            }
            for result in results
        ],
        "notifications": [
            {"recipient": notification.recipient, "text": notification.text}
            for notification in sink.sent
        ],
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if any(result.failed for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
