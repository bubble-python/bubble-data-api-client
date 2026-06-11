from __future__ import annotations

import asyncio
import typing
from datetime import datetime

import pytest

from bubble_data_api_client.constraints import AdditionalSortField, Constraint, ConstraintType, constraint
from bubble_data_api_client.pagination import keyset_scan
from bubble_data_api_client.types import PageResult

_FIELD = "Created Date"


def _row(idx: int, ts: str, **extra: typing.Any) -> dict[str, typing.Any]:
    return {"_id": f"id{idx:03d}", _FIELD: ts, **extra}


def _ts(second: int) -> str:
    """Build a naive ISO timestamp with the given second offset."""
    return f"2024-01-01T00:00:{second:02d}"


class FakeBubble:
    """In-memory stand-in that emulates Bubble's offset + seek query semantics.

    Applies a greater-than seek bound and base equals constraints, sorts by
    (sort_field, _id) to match the engine's tiebreaker, then slices by cursor
    and limit. Records every call so tests can assert that a seek happened.
    """

    def __init__(self, rows: list[dict[str, typing.Any]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, typing.Any]] = []

    async def fetch(
        self,
        *,
        constraints: list[Constraint] | None,
        cursor: int,
        limit: int,
        sort_field: str,
        additional_sort_fields: list[AdditionalSortField],
    ) -> PageResult[dict[str, typing.Any]]:
        self.calls.append({"cursor": cursor, "constraints": list(constraints or [])})

        selected = list(self.rows)
        for c in constraints or []:
            ctype = c["constraint_type"]
            key = c["key"]
            if ctype == ConstraintType.GREATER_THAN:
                bound = typing.cast("datetime", c.get("value"))
                selected = [r for r in selected if datetime.fromisoformat(r[key]) > bound]
            elif ctype == ConstraintType.EQUALS:
                selected = [r for r in selected if r.get(key) == c.get("value")]

        selected.sort(key=lambda r: (r[sort_field], r["_id"]))
        page = selected[cursor : cursor + limit]
        remaining = len(selected) - (cursor + len(page))
        return PageResult(items=page, cursor=cursor, remaining=remaining)


def _seek_happened(fake: FakeBubble) -> bool:
    return any(c["constraint_type"] == ConstraintType.GREATER_THAN for call in fake.calls for c in call["constraints"])


async def test_empty_dataset_yields_nothing() -> None:
    fake = FakeBubble([])

    rows = [r async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=100)]

    assert rows == []
    assert len(fake.calls) == 1  # one fetch, sees empty page, stops


async def test_single_page_no_seek() -> None:
    fake = FakeBubble([_row(0, _ts(1)), _row(1, _ts(2)), _row(2, _ts(3))])

    ids = [r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=100)]

    assert ids == ["id000", "id001", "id002"]
    assert not _seek_happened(fake)


async def test_distinct_dates_seek_across_window() -> None:
    """Small window forces seeking; every distinct-dated row appears once, in order."""
    data = [_row(i, _ts(i + 1)) for i in range(10)]
    fake = FakeBubble(data)

    ids = [r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=2)]

    assert ids == [f"id{i:03d}" for i in range(10)]
    assert len(ids) == len(set(ids))  # no duplicates
    assert _seek_happened(fake)  # the cursor did not simply grow unbounded


async def test_dense_bucket_pages_by_offset_until_a_second_date_appears() -> None:
    """A timestamp shared by more rows than the window must not crash or skip rows.

    While only one distinct date has been seen there is no strict lower bound to
    seek from, so the engine keeps paging by offset through the bucket instead.
    """
    data = [_row(i, _ts(1)) for i in range(5)] + [_row(5, _ts(2)), _row(6, _ts(2))]
    fake = FakeBubble(data)

    ids = [r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=2)]

    assert ids == [f"id{i:03d}" for i in range(7)]
    assert len(ids) == len(set(ids))


async def test_bucket_straddling_window_dedupes_seek_overlap() -> None:
    """When a seek re-fetches a partially-consumed bucket, the overlap is deduped."""
    data = [_row(i, _ts(1)) for i in range(4)] + [_row(i, _ts(2)) for i in range(4, 8)]
    fake = FakeBubble(data)

    ids = [r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=3, window=3)]

    assert ids == [f"id{i:03d}" for i in range(8)]
    assert len(ids) == len(set(ids))
    assert _seek_happened(fake)


async def test_bucket_wider_than_window_terminates() -> None:
    """A bucket wider than the window, reached after a seek, must not loop forever.

    Regression: the seek bound is the second-most-recent date, which only advances
    when a new distinct date appears. Once a first date has set the bound, a later
    bucket wider than the window yields no new date within a window, so re-seeking
    would re-fetch the same already-seen rows endlessly. The engine must instead
    keep paging by offset through the dense bucket until a new value appears.
    """
    # two rows at the first date set the seek bound, then a bucket far wider than
    # the window, then a trailing distinct date the offset paging must still reach.
    data = [_row(i, _ts(1)) for i in range(2)] + [_row(i, _ts(2)) for i in range(2, 22)] + [_row(22, _ts(3))]
    fake = FakeBubble(data)

    ids = [r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=3)]

    assert ids == [f"id{i:03d}" for i in range(23)]
    assert len(ids) == len(set(ids))


async def test_base_constraints_are_preserved_through_seeks() -> None:
    """Caller filters survive every seek; only matching rows are yielded."""
    data = [_row(i, _ts(i + 1), group="A" if i % 2 == 0 else "B") for i in range(8)]
    fake = FakeBubble(data)

    base = [constraint(key="group", constraint_type=ConstraintType.EQUALS, value="A")]
    ids = [
        r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=1, window=1, constraints=base)
    ]

    assert ids == ["id000", "id002", "id004", "id006"]
    # every fetch keeps the caller's equals filter alongside any seek bound
    assert all(
        any(c["constraint_type"] == ConstraintType.EQUALS and c["value"] == "A" for c in call["constraints"])
        for call in fake.calls
    )


async def test_stops_on_empty_page_past_offset_cap() -> None:
    """Simulate the cap: an empty page with remaining > 0 must terminate the scan.

    A single timestamp bucket larger than the offset cap cannot be paged past;
    Bubble returns no rows while still reporting remaining, and the scan must
    stop rather than loop forever.
    """
    pages = iter(
        [
            PageResult(items=[_row(0, _ts(1))], cursor=0, remaining=5),
            PageResult(items=[], cursor=1, remaining=5),  # past the cap
        ]
    )

    async def fetch(**_kwargs: typing.Any) -> PageResult[dict[str, typing.Any]]:
        return next(pages)

    ids = [r["_id"] async for r in keyset_scan(fetch, keyset_field=_FIELD, page_size=1, window=1000)]

    assert ids == ["id000"]


# --- concurrency ---


def _ts_minutes(idx: int) -> str:
    """Build a naive ISO timestamp distinct for indexes beyond one minute."""
    return f"2024-01-01T00:{idx // 60:02d}:{idx % 60:02d}"


class InFlightProbe(FakeBubble):
    """FakeBubble that records how many fetches were ever in flight at once."""

    def __init__(self, rows: list[dict[str, typing.Any]]) -> None:
        super().__init__(rows)
        self.in_flight = 0
        self.max_in_flight = 0

    async def fetch(self, **kwargs: typing.Any) -> PageResult[dict[str, typing.Any]]:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        # yield to the event loop so concurrently dispatched fetches overlap
        await asyncio.sleep(0)
        try:
            return await super().fetch(**kwargs)
        finally:
            self.in_flight -= 1


@pytest.mark.parametrize("concurrency", [2, 3, 7])
async def test_concurrency_yields_same_rows_through_seeks(concurrency: int) -> None:
    """Concurrent batches must reproduce the sequential output exactly: same
    rows, same order, no duplicates, across seeks and a dense bucket."""
    data = (
        [_row(i, _ts(1)) for i in range(5)]  # dense bucket wider than the window
        + [_row(i, _ts(i - 3)) for i in range(5, 20)]  # distinct dates forcing seeks
    )
    expected = [r["_id"] async for r in keyset_scan(FakeBubble(data).fetch, keyset_field=_FIELD, page_size=2, window=4)]
    fake = FakeBubble(data)

    ids = [
        r["_id"]
        async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=4, concurrency=concurrency)
    ]

    assert ids == expected == [f"id{i:03d}" for i in range(20)]
    assert _seek_happened(fake)


async def test_concurrency_fetches_pages_in_parallel() -> None:
    """Batches after the probe page actually overlap their fetches."""
    data = [_row(i, _ts_minutes(i)) for i in range(30)]
    fake = InFlightProbe(data)

    ids = [
        r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=1000, concurrency=4)
    ]

    assert ids == [f"id{i:03d}" for i in range(30)]
    assert fake.max_in_flight == 4


async def test_concurrency_sizes_batches_by_remaining() -> None:
    """The probe page's remaining caps fan-out, so no fetch targets offsets
    known to be empty: 5 rows at page_size 2 need exactly 3 fetches."""
    data = [_row(i, _ts(i + 1)) for i in range(5)]
    fake = FakeBubble(data)

    ids = [
        r["_id"] async for r in keyset_scan(fake.fetch, keyset_field=_FIELD, page_size=2, window=1000, concurrency=10)
    ]

    assert ids == [f"id{i:03d}" for i in range(5)]
    assert [call["cursor"] for call in fake.calls] == [0, 2, 4]


async def test_concurrency_short_page_discards_misaligned_batch_pages() -> None:
    """A short page while rows remain invalidates the batch's later offsets.

    Later pages in the same batch were fetched assuming the short page was
    full, so they skip the rows the short page failed to deliver. The engine
    must discard them and refetch from the corrected cursor: every row is
    yielded exactly once, in order.
    """
    data = [_row(i, _ts_minutes(i)) for i in range(10)]
    inner = FakeBubble(data)
    truncated: list[int] = []

    async def fetch(**kwargs: typing.Any) -> PageResult[dict[str, typing.Any]]:
        page = await inner.fetch(**kwargs)
        # serve the first fetch at cursor 2 one row short while rows remain,
        # emulating a concurrent deletion shifting offsets mid-batch
        if kwargs["cursor"] == 2 and not truncated:
            truncated.append(kwargs["cursor"])
            return PageResult(items=page.items[:1], cursor=page.cursor, remaining=page.remaining + 1)
        return page

    ids = [r["_id"] async for r in keyset_scan(fetch, keyset_field=_FIELD, page_size=2, window=1000, concurrency=3)]

    assert ids == [f"id{i:03d}" for i in range(10)]
    assert len(ids) == len(set(ids))
    # the misaligned pages at cursors 4 and 6 were discarded; the refetch
    # resumed from the corrected cursor 3
    assert 3 in [call["cursor"] for call in inner.calls]


async def test_concurrency_error_propagates_after_preceding_pages() -> None:
    """A failed fetch raises only after the rows before it were yielded,
    matching the prefix sequential fetching would have produced."""
    data = [_row(i, _ts_minutes(i)) for i in range(10)]
    inner = FakeBubble(data)

    async def fetch(**kwargs: typing.Any) -> PageResult[dict[str, typing.Any]]:
        if kwargs["cursor"] == 4:
            msg = "boom"
            raise RuntimeError(msg)
        return await inner.fetch(**kwargs)

    ids: list[str] = []
    with pytest.raises(RuntimeError, match="boom"):
        async for row in keyset_scan(fetch, keyset_field=_FIELD, page_size=2, window=1000, concurrency=3):
            # a comprehension would discard the partial rows when the scan
            # raises, and those are exactly what this test asserts on
            ids.append(row["_id"])  # noqa: PERF401

    # probe page (cursor 0) and the batch page before the failure (cursor 2)
    assert ids == [f"id{i:03d}" for i in range(4)]


async def test_concurrency_must_be_positive() -> None:
    with pytest.raises(ValueError, match="concurrency"):
        _ = [r async for r in keyset_scan(FakeBubble([]).fetch, keyset_field=_FIELD, page_size=2, concurrency=0)]
