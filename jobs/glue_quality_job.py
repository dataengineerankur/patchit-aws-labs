# PATCHIT auto-fix: fix_job_bookmark
# Original error: awsglue.utils.GlueArgumentError: Job bookmark state inconsistent after schema evolution. Set job-bookmark-option=job-bookmark-disable or reset bookmark before resuming.
from __future__ import annotations

# Secondary Glue job for quality checks.

from datetime import datetime


def quality_metrics(rows: list[dict]) -> dict:
    total = len(rows)
    null_keys = sum(1 for r in rows if r.get("id") is None)
    return {
        "total_rows": total,
        "null_key_rows": null_keys,
        "ts": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    sample = [
        {"id": 1, "event_ts": "2025-01-01T00:00:00Z"},
        {"id": None, "event_ts": "2025-01-01T01:00:00Z"},
    ]
    print(quality_metrics(sample))
