from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx

from bubble_data_api_client.client.orm import BubbleModel
from bubble_data_api_client.client.raw_client import RawClient
from bubble_data_api_client.constraints import ConstraintType, constraint

if TYPE_CHECKING:
    import respx


def _bubble_handler(rows: list[dict[str, Any]]):
    """Serve rows like Bubble: apply seek/equals constraints, sort, slice by cursor."""

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        cursor = int(params.get("cursor", "0"))
        limit = int(params.get("limit", "100"))
        sort_field = params.get("sort_field")
        raw_constraints = params.get("constraints")
        constraints = json.loads(raw_constraints) if raw_constraints else []

        selected = list(rows)
        for c in constraints:
            if c["constraint_type"] == "greater than":
                bound = datetime.fromisoformat(c["value"])
                selected = [r for r in selected if datetime.fromisoformat(r[c["key"]]) > bound]
            elif c["constraint_type"] == "equals":
                selected = [r for r in selected if r.get(c["key"]) == c["value"]]

        selected.sort(key=lambda r: (r[sort_field], r["_id"]))
        page = selected[cursor : cursor + limit]
        remaining = len(selected) - (cursor + len(page))
        return httpx.Response(
            200,
            json={"response": {"cursor": cursor, "results": page, "count": len(page), "remaining": remaining}},
        )

    return handler


def _ts(second: int) -> str:
    return f"2024-01-01T00:00:{second:02d}"


async def test_raw_scan_seeks_through_all_rows(configured_client: None, httpx2_mock: respx.Router) -> None:
    """RawClient.scan streams every row in Created Date order across a forced seek."""
    rows = [{"_id": f"id{i}", "Created Date": _ts(i + 1), "name": f"n{i}"} for i in range(6)]
    route = httpx2_mock.get("https://example.com/widget")
    route.side_effect = _bubble_handler(rows)

    async with RawClient() as client:
        scanned = [r["_id"] async for r in client.scan("widget", page_size=2, window=2)]

    assert scanned == [f"id{i}" for i in range(6)]
    assert len(scanned) == len(set(scanned))
    # the small window forced at least one forward seek rather than unbounded offset
    assert any("greater" in str(call.request.url) for call in route.calls)


async def test_raw_scan_forwards_base_constraints(configured_client: None, httpx2_mock: respx.Router) -> None:
    """A caller filter is preserved on every page of a scan."""
    rows = [{"_id": f"id{i}", "Created Date": _ts(i + 1), "group": "A" if i % 2 == 0 else "B"} for i in range(6)]
    route = httpx2_mock.get("https://example.com/widget")
    route.side_effect = _bubble_handler(rows)

    async with RawClient() as client:
        scanned = [
            r["_id"]
            async for r in client.scan(
                "widget",
                constraints=[constraint(key="group", constraint_type=ConstraintType.EQUALS, value="A")],
                page_size=1,
                window=1,
            )
        ]

    assert scanned == ["id0", "id2", "id4"]


async def test_raw_scan_concurrency_yields_same_rows(configured_client: None, httpx2_mock: respx.Router) -> None:
    """concurrency > 1 streams the same rows in the same order as sequential."""
    rows = [{"_id": f"id{i:02d}", "Created Date": _ts(i + 1), "name": f"n{i}"} for i in range(12)]
    route = httpx2_mock.get("https://example.com/widget")
    route.side_effect = _bubble_handler(rows)

    async with RawClient() as client:
        scanned = [r["_id"] async for r in client.scan("widget", page_size=2, window=4, concurrency=3)]

    assert scanned == [f"id{i:02d}" for i in range(12)]
    assert len(scanned) == len(set(scanned))


async def test_orm_scan_returns_typed_models_through_seek(configured_client: None, httpx2_mock: respx.Router) -> None:
    """BubbleModel.scan yields validated instances across a seek, in Created Date order."""

    class Widget(BubbleModel, typename="widget"):
        name: str

    rows = [{"_id": f"id{i}", "Created Date": _ts(i + 1), "name": f"n{i}"} for i in range(5)]
    route = httpx2_mock.get("https://example.com/widget")
    route.side_effect = _bubble_handler(rows)

    widgets = [w async for w in Widget.scan(page_size=2, window=2, concurrency=2)]

    assert all(isinstance(w, Widget) for w in widgets)
    assert [w.uid for w in widgets] == [f"id{i}" for i in range(5)]
    assert [w.name for w in widgets] == [f"n{i}" for i in range(5)]


async def test_orm_scan_single_page(configured_client: None, httpx2_mock: respx.Router) -> None:
    """A scan that fits in one page returns all rows and stops."""

    class Widget(BubbleModel, typename="widget"):
        name: str

    httpx2_mock.get("https://example.com/widget").respond(
        200,
        json={
            "response": {
                "cursor": 0,
                "results": [
                    {"_id": "id0", "Created Date": _ts(1), "name": "Alice"},
                    {"_id": "id1", "Created Date": _ts(2), "name": "Bob"},
                ],
                "count": 2,
                "remaining": 0,
            }
        },
    )

    widgets = [w async for w in Widget.scan()]

    assert [w.name for w in widgets] == ["Alice", "Bob"]


async def test_orm_scan_empty(configured_client: None, httpx2_mock: respx.Router) -> None:
    """A scan over an empty collection yields nothing."""

    class Widget(BubbleModel, typename="widget"):
        name: str

    httpx2_mock.get("https://example.com/widget").respond(
        200,
        json={"response": {"cursor": 0, "results": [], "count": 0, "remaining": 0}},
    )

    widgets = [w async for w in Widget.scan()]

    assert widgets == []
