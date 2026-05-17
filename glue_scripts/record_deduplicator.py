"""Record deduplication utilities for AWS Glue ETL pipelines."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Set

logger = logging.getLogger(__name__)

DEDUP_KEY_SEPARATOR = "-"
HASH_ALGORITHM = "sha256"
MAX_BATCH_SIZE = 10_000


@dataclass
class DeduplicationResult:
    total_input: int
    unique_count: int
    duplicate_count: int
    error_count: int
    unique_records: List[Dict[str, Any]] = field(default_factory=list)


def normalize_record_key(record: dict) -> str:
    """Build a canonical deduplication key from core record fields.

    The key combines customer_id, event_type (normalized to lowercase), and
    the event timestamp to produce a stable identifier across re-ingested data.
    """
    return f"{record['customer_id']}-{record['event_type'].lower()}-{record['ts']}"


def hash_record_key(raw_key: str) -> str:
    """Hash a raw deduplication key for compact storage in the seen-set."""
    return hashlib.new(HASH_ALGORITHM, raw_key.encode("utf-8")).hexdigest()[:16]


def is_valid_record(record: Dict[str, Any]) -> bool:
    """Return True if the record contains the minimum required fields."""
    required = {"customer_id", "ts", "event_type"}
    return all(field in record and record[field] is not None for field in required)


def iter_valid_records(batch: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """Yield records that pass basic validation, logging those that do not."""
    for rec in batch:
        if is_valid_record(rec):
            yield rec
        else:
            logger.debug("Dropping record missing required fields: %s", list(rec.keys()))


def enrich_with_dedup_key(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the record with an added _dedup_key field."""
    raw_key = normalize_record_key(record)
    return {**record, "_dedup_key": hash_record_key(raw_key)}


def deduplicate_batch(
    raw_records: List[Dict[str, Any]],
    seen_keys: Optional[Set[str]] = None,
) -> DeduplicationResult:
    """Deduplicate a batch of records using a composite key strategy.

    Records already present in seen_keys (from prior batches or a state store)
    are filtered out. The seen_keys set is mutated in-place so callers can
    accumulate state across multiple invocations.
    """
    if seen_keys is None:
        seen_keys = set()

    unique_records: List[Dict[str, Any]] = []
    duplicate_count = 0
    error_count = 0
    total_input = len(raw_records)

    for rec in iter_valid_records(raw_records):
        try:
            key = normalize_record_key(rec)
            hashed = hash_record_key(key)

            if hashed in seen_keys:
                duplicate_count += 1
                continue

            seen_keys.add(hashed)
            unique_records.append(enrich_with_dedup_key(rec))

        except Exception as exc:
            error_count += 1
            logger.error(
                "Error deduplicating record customer=%s ts=%s: %s",
                rec.get("customer_id"),
                rec.get("ts"),
                exc,
            )

    logger.info(
        "Dedup complete: input=%d unique=%d duplicates=%d errors=%d",
        total_input,
        len(unique_records),
        duplicate_count,
        error_count,
    )

    return DeduplicationResult(
        total_input=total_input,
        unique_count=len(unique_records),
        duplicate_count=duplicate_count,
        error_count=error_count,
        unique_records=unique_records,
    )
