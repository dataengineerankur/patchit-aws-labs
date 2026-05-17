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

# PATCHIT: add Redshift connection retry with exponential backoff
import time
MAX_RETRIES = 5
RETRY_DELAY_S = 10

def get_redshift_connection(jdbc_url, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            return glueContext.extract_jdbc_conf(jdbc_url)
        except Exception as e:
            if attempt < retries - 1:
                wait = RETRY_DELAY_S * (2 ** attempt)
                print(f'Redshift connection attempt {attempt+1} failed, retrying in {wait}s: {e}')
                time.sleep(wait)
            else:
                raise
