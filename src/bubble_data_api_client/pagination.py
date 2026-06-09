"""Keyset (seek) pagination for collections beyond Bubble's offset cap.

Bubble's offset pagination (cursor + limit) silently stops returning rows once
the cursor exceeds roughly 50,000 on shared infrastructure, while still
reporting a non-zero remaining. To stream a collection of any size, this module
walks rows in ascending order of a monotonic date field (Created Date by
default) and seeks forward with a "greater than" constraint instead of letting
the cursor grow without bound.

The engine is pure: it takes a page-fetch callback and yields raw dicts, with no
knowledge of HTTP, models, or configuration. The boundary-tracking logic that
makes keyset pagination correct over a non-unique sort key lives here once, so
RawClient and BubbleModel both reuse it.

Inherent limits:
    Ordering is fixed to the keyset field. An arbitrary caller sort cannot be
    combined with cap-free iteration, because the seek predicate must be on the
    sort field and Bubble's AND-only constraints cannot express the compound
    "(date > x) OR (date = x AND _id > y)" a secondary sort would require.

    A single keyset value (timestamp) shared by more rows than the offset cap
    (~50,000) cannot be paged past: the seek cannot advance without skipping
    un-fetched rows at that value, and the cursor caps out within the bucket.
    Iteration stops short there, the same hard limit offset pagination hits.
"""

from __future__ import annotations

import typing
from datetime import datetime

from bubble_data_api_client.constraints import ConstraintType, constraint, sort_by
from bubble_data_api_client.types import BubbleField

if typing.TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from bubble_data_api_client.constraints import AdditionalSortField, Constraint
    from bubble_data_api_client.types import PageResult

# cursor offset at which to seek forward instead of paging further. high cursor
# values get slow and approach Bubble's ~50,000 offset cap, so we reset to 0
# with a greater-than constraint well before reaching it. kept high enough that
# a single dense timestamp bucket is still paged through by offset between seeks.
_DEFAULT_KEYSET_WINDOW: int = 1000


class _PageFetch(typing.Protocol):
    """Fetches one page in the fixed ascending sort order keyset requires."""

    def __call__(
        self,
        *,
        constraints: list[Constraint] | None,
        cursor: int,
        limit: int,
        sort_field: str,
        additional_sort_fields: list[AdditionalSortField],
    ) -> typing.Awaitable[PageResult[dict[str, typing.Any]]]: ...


async def keyset_scan(
    fetch: _PageFetch,
    *,
    keyset_field: str,
    page_size: int,
    constraints: list[Constraint] | None = None,
    window: int = _DEFAULT_KEYSET_WINDOW,
) -> AsyncIterator[dict[str, typing.Any]]:
    """Yield every row matching constraints, ordered by keyset_field ascending.

    Pages by offset within a window, then seeks forward past the window with a
    greater-than constraint so the cursor never approaches Bubble's offset cap.
    Rows re-fetched by the seek overlap are deduplicated, so no row is yielded
    twice. Memory is bounded by one timestamp bucket, not the full result set.

    Args:
        fetch: Callback returning one PageResult for the given constraints,
            cursor, and sort order. Must sort ascending by sort_field.
        keyset_field: Monotonic date field to page by (e.g. "Created Date").
            Values must be ISO 8601 strings parseable by datetime.fromisoformat.
        page_size: Rows requested per page (Bubble caps this at 100).
        constraints: Caller filters, preserved and combined with the seek bound.
        window: Cursor offset at which to seek forward. Must be below Bubble's
            ~50,000 offset cap.

    Yields:
        Raw row dicts in ascending keyset_field order.
    """
    base: list[Constraint] = list(constraints) if constraints else []
    # the _id tiebreaker makes the order total and stable: Bubble's intra-tie
    # order can otherwise shift between requests, skipping or duplicating rows.
    tiebreak: list[AdditionalSortField] = [sort_by(BubbleField.ID)]

    boundary: list[Constraint] = base
    cursor: int = 0
    # the largest keyset value yielded so far, and the largest distinct value
    # strictly below it. seeking from second_last (not last) re-includes every
    # row sharing last's value, since Bubble offers "greater than" but not
    # "greater than or equal".
    last: datetime | None = None
    second_last: datetime | None = None
    # the bound used by the most recent seek. a seek only makes progress if it
    # advances this, so re-seeking with an unchanged second_last is suppressed.
    last_seek_bound: datetime | None = None
    # ids already yielded at value == last, the only rows a seek can re-return.
    # reset whenever last advances, so it never grows past one timestamp bucket.
    boundary_ids: set[str] = set()

    while True:
        page = await fetch(
            constraints=boundary,
            cursor=cursor,
            limit=page_size,
            sort_field=keyset_field,
            additional_sort_fields=tiebreak,
        )
        # an empty page ends iteration. past the offset cap Bubble returns no
        # rows while still reporting remaining > 0, so this guard, not remaining,
        # is what terminates a scan that hits the cap.
        if not page.items:
            return

        for row in page.items:
            row_id: str = row[BubbleField.ID]
            if row_id in boundary_ids:
                continue  # duplicate carried in by the seek overlap
            value = datetime.fromisoformat(row[keyset_field])
            if last is None or value > last:
                second_last, last = last, value
                boundary_ids = {row_id}
            else:  # value == last, same bucket
                boundary_ids.add(row_id)
            yield row

        if page.remaining == 0:
            return

        cursor += len(page.items)

        # seek forward only when it would advance past where the last seek left
        # off. two cases keep us paging by offset instead: second_last is None
        # (only one distinct value seen, no strict lower bound yet), or it equals
        # last_seek_bound (a single bucket wider than the window, so no new
        # distinct value has appeared since the last seek). re-seeking with an
        # unchanged bound would re-fetch the same rows forever, so we let the
        # cursor grow through the dense bucket until a new value advances the
        # bound or the offset cap returns an empty page.
        if cursor >= window and second_last is not None and second_last != last_seek_bound:
            cursor = 0
            last_seek_bound = second_last
            boundary = [
                *base,
                constraint(key=keyset_field, constraint_type=ConstraintType.GREATER_THAN, value=second_last),
            ]
