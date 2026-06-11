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

    Rows deleted while a scan runs can shift offsets between two fetches of
    the same window without leaving a detectable trace (no page comes back
    short), silently skipping a row. This is inherent to offset pagination
    over live data and applies equally to sequential and concurrent fetching;
    a seek re-anchors on the keyset value, so the exposure is bounded by one
    window, not the whole scan. Inserts are safe with a creation-time keyset
    field: new rows sort after the scan position and are picked up normally.
"""

from __future__ import annotations

import asyncio
import math
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
_DEFAULT_KEYSET_WINDOW: typing.Final[int] = 1000

# pages fetched in parallel per batch. 1 keeps scans strictly sequential, the
# safe default for rate-limited plans; Bubble has been measured to serve at
# least 20 concurrent page queries with near-linear throughput scaling, so
# callers can raise this explicitly when they own the request budget.
_DEFAULT_SCAN_CONCURRENCY: typing.Final[int] = 1


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


async def _fetch_batch(
    fetch: _PageFetch,
    *,
    constraints: list[Constraint],
    start_cursor: int,
    count: int,
    page_size: int,
    sort_field: str,
    additional_sort_fields: list[AdditionalSortField],
) -> list[PageResult[dict[str, typing.Any]] | BaseException]:
    """Fetch count pages at contiguous cursors, concurrently when count > 1.

    Returns pages in cursor order. A failed concurrent fetch is returned in
    place as the exception rather than raised, so the caller can process the
    pages that precede it (matching what sequential fetching would have
    yielded) before propagating the error; the single-page path raises
    directly, which is equivalent because no preceding pages exist. All
    fetches have completed by the time this returns, so no task is left in
    flight if the caller's generator is closed.
    """

    def one(index: int) -> typing.Awaitable[PageResult[dict[str, typing.Any]]]:
        return fetch(
            constraints=constraints,
            cursor=start_cursor + index * page_size,
            limit=page_size,
            sort_field=sort_field,
            additional_sort_fields=additional_sort_fields,
        )

    if count == 1:
        return [await one(0)]
    return await asyncio.gather(*(one(i) for i in range(count)), return_exceptions=True)


async def keyset_scan(
    fetch: _PageFetch,
    *,
    keyset_field: str,
    page_size: int,
    constraints: list[Constraint] | None = None,
    window: int = _DEFAULT_KEYSET_WINDOW,
    concurrency: int = _DEFAULT_SCAN_CONCURRENCY,
) -> AsyncIterator[dict[str, typing.Any]]:
    """Yield every row matching constraints, ordered by keyset_field ascending.

    Pages by offset within a window, then seeks forward past the window with a
    greater-than constraint so the cursor never approaches Bubble's offset cap.
    Rows re-fetched by the seek overlap are deduplicated, so no row is yielded
    twice. Memory is bounded by one batch of pages plus one timestamp bucket,
    not the full result set.

    With concurrency > 1, pages at contiguous cursors are fetched in parallel
    and processed in cursor order, so ordering and dedup behave exactly as in
    the sequential case while throughput scales with concurrency (page latency
    is dominated by Bubble's per-query time). The first page of a scan is
    always fetched alone: its remaining count sizes every following batch, so
    a scan never fans out into offsets known to be empty. If a page comes back
    short while rows remain (a concurrent deletion shifted offsets), the rest
    of its batch is discarded and refetched from the corrected cursor: the
    detectable shift causes rows to be fetched twice rather than skipped, and
    none are yielded twice. Deletions that shift offsets without shortening
    any page are undetectable and can skip a row, sequentially or not; see
    the module docstring.

    Args:
        fetch: Callback returning one PageResult for the given constraints,
            cursor, and sort order. Must sort ascending by sort_field.
        keyset_field: Monotonic date field to page by (e.g. "Created Date").
            Values must be ISO 8601 strings parseable by datetime.fromisoformat.
        page_size: Rows requested per page (Bubble caps this at 100).
        constraints: Caller filters, preserved and combined with the seek bound.
        window: Cursor offset at which to seek forward. The seek check runs
            after each batch, so the cursor can overshoot the window by up to
            concurrency * page_size; window plus that overshoot must stay
            below Bubble's ~50,000 offset cap.
        concurrency: Maximum pages fetched in parallel per batch. 1 preserves
            strictly sequential fetching. Higher values multiply request rate
            against Bubble, which may matter for rate-limited plans.

    Yields:
        Raw row dicts in ascending keyset_field order.

    Raises:
        ValueError: If concurrency is less than 1.
    """
    if concurrency < 1:
        msg = f"concurrency must be >= 1, got {concurrency}"
        raise ValueError(msg)

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
    # remaining reported by the last processed page, used only to size the next
    # batch. None until the first page reports it, so a scan always starts with
    # a single probe page. After a seek it slightly undercounts (the seek
    # re-includes the boundary bucket), which only shrinks one batch.
    remaining_hint: int | None = None

    while True:
        batch_size: int = 1 if remaining_hint is None else min(concurrency, math.ceil(remaining_hint / page_size))
        pages = await _fetch_batch(
            fetch,
            constraints=boundary,
            start_cursor=cursor,
            count=batch_size,
            page_size=page_size,
            sort_field=keyset_field,
            additional_sort_fields=tiebreak,
        )

        for item in pages:
            # a failed fetch propagates only after the pages before it were
            # yielded, the same prefix sequential fetching would have produced.
            if isinstance(item, BaseException):
                raise item
            page = item
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

            remaining_hint = page.remaining
            cursor += len(page.items)

            # a short page while rows remain means later pages in this batch
            # were fetched at offsets that assumed a full page and may skip
            # rows. discard them and refetch from the corrected cursor: rows
            # may be fetched twice, but none are skipped or yielded twice.
            if len(page.items) < page_size:
                break

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
