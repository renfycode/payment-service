import time
from datetime import UTC, datetime, timedelta
from uuid import UUID

import uuid_utils.compat as uuid

from payments.messaging.outbox_cleanup import uuid7_lower_bound


def test_bound_separates_uuid7_created_before_and_after() -> None:
    before = uuid.uuid7()
    time.sleep(0.005)
    boundary = uuid7_lower_bound(datetime.now(UTC))
    time.sleep(0.005)
    after = uuid.uuid7()

    assert before < boundary < after


def test_bound_is_a_valid_uuid7_with_the_given_time() -> None:
    moment = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    boundary = uuid7_lower_bound(moment)

    assert boundary.version == 7
    assert boundary.int >> 80 == int(moment.timestamp() * 1000)


def test_bounds_are_ordered_by_time() -> None:
    now = datetime.now(UTC)

    bounds = [uuid7_lower_bound(now - timedelta(days=d)) for d in (365, 180, 1, 0)]

    assert bounds == sorted(bounds)
    assert all(isinstance(b, UUID) for b in bounds)
