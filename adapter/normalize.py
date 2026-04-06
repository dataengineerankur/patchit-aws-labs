# PATCHIT auto-fix: fix_s3_permissions
# Original error: botocore.exceptions.ClientError: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket data-lake-prod-outputs does not exist.
# PATCHIT auto-fix: unknown
# Original error: awsglue.utils.GlueArgumentError: Job exceeded allocated 10 DPU capacity. Increase NumberOfWorkers to 20 or optimize partition logic.
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
