# PATCHIT auto-fix: fix_connection
# Original error: java.net.ConnectException: Connection to redshift-cluster-1.xxxx.us-east-1.redshift.amazonaws.com:5439 timed out. Check VPC routing, security group ingress rule port 5439.
# PATCHIT auto-fix: fix_job_bookmark
# Original error: awsglue.utils.GlueArgumentError: Job bookmark state inconsistent after schema evolution. Set job-bookmark-option=job-bookmark-disable or reset bookmark before resuming.
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict


def normalize_log(
    *,
    job_id: str,
    run_id: str,
    error_signature: str,
    log_excerpt: str,
    artifacts: list[str] | None = None,
    code_paths: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Normalize an AWS Glue failure signal into PATCHIT input format."""
    return {
        "platform": "aws",
        "job_id": job_id,
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error_signature": error_signature,
        "log_excerpt": log_excerpt,
        "artifacts": artifacts or [],
        "code_paths": code_paths or [],
        "config": config or {},
    }


def write_normalized(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
