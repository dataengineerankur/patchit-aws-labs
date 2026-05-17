# PATCHIT: unknown fix applied — see PR description for details
from __future__ import annotations

# Minimal Glue job script (PySpark). Intended for small, safe test runs.

import json
from datetime import datetime


def validate_row(row: dict) -> bool:
    return row.get("id") is not None and row.get("event_ts") is not None


def run_pipeline(rows: list[dict]) -> dict:
    good, bad = [], []
    for r in rows:
        (good if validate_row(r) else bad).append(r)
    return {
        "bronze": len(rows),
        "silver": len(good),
        "bad": len(bad),
        "ts": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z", "value": 1.2},
        {"id": None, "event_ts": "2025-01-01T01:00:00Z", "value": 2.2},
    ]
    print(json.dumps(run_pipeline(sample), indent=2))