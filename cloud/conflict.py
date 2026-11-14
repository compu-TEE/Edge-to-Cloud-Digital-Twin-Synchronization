"""Conflict-resolution rule shared (byte-identical) by the edge and the cloud.

Policy: per-property Last-Writer-Wins register ordered by the total order
    (event_time, event_id)

* event_time is compared as an absolute UTC instant (timezone aware).
* event_id is a deterministic tie-breaker for identical timestamps.

Because the order is total, `wins` is commutative, associative and idempotent
over a set of events: whatever order (or number of times) the two tiers see the
same events, both converge to the same value.  This is a state-based LWW-Register
CRDT applied independently to every (twin_id, property) pair.

tests/test_core.py asserts that edge/conflict.py and cloud/conflict.py are
identical, so the two tiers can never silently diverge in their rule.
"""
from datetime import datetime, timezone


def parse_ts(value):
    """Parse an ISO-8601 timestamp to an aware UTC datetime (naive => UTC)."""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def wins(in_time, in_id, cur_time, cur_id):
    """True if the incoming event must replace the current property value."""
    if cur_time is None:
        return True
    a, b = parse_ts(in_time), parse_ts(cur_time)
    return a > b or (a == b and str(in_id) > str(cur_id))
