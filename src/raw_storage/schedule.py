"""Pure schedule-slot validation and deterministic Raw run identities.

A key identifies the DAG, stage and scope. The timestamp identifies one
scheduled occurrence; invocation time and Airflow retry numbers are excluded.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from uuid import NAMESPACE_URL, UUID, uuid5


def normalize_schedule_slot(schedule_key=None, scheduled_for=None):
    """Return a complete slot in UTC, or ``(None, None)`` for legacy runs."""
    if schedule_key is None and scheduled_for is None:
        return None, None
    if (not isinstance(schedule_key, str) or not schedule_key.strip()
            or schedule_key != schedule_key.strip()):
        raise ValueError('schedule_key must be a nonempty string without surrounding whitespace.')
    if (not isinstance(scheduled_for, datetime) or scheduled_for.tzinfo is None
            or scheduled_for.utcoffset() is None):
        raise ValueError('scheduled_for must be a timezone-aware datetime paired with schedule_key.')
    return schedule_key, scheduled_for.astimezone(timezone.utc)


def scheduled_run_id(schedule_key, scheduled_for, *, source_code='topcv') -> UUID:
    """The same source/key/instant always yields the same UUID in every worker."""
    key, instant = normalize_schedule_slot(schedule_key, scheduled_for)
    if key is None:
        raise ValueError('A scheduled run needs a complete schedule slot.')
    if (not isinstance(source_code, str) or not source_code.strip()
            or source_code != source_code.strip()):
        raise ValueError('source_code must be a nonempty canonical string.')
    identity = json.dumps(
        [source_code, key, instant.isoformat(timespec='microseconds')],
        ensure_ascii=False, separators=(',', ':'),
    )
    return uuid5(NAMESPACE_URL, 'topcv-raw-schedule-v1:' + identity)
