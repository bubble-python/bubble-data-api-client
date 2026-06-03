from __future__ import annotations

import json
import urllib.parse
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic_core import PydanticSerializationError

from bubble_data_api_client import BubbleAPIError
from bubble_data_api_client.client import raw_client

if TYPE_CHECKING:
    import respx


async def test_raw_client_init() -> None:
    """Test that RawClient can be instantiated and used as context manager."""
    # test creating an instance
    client = raw_client.RawClient()
    assert isinstance(client, raw_client.RawClient)

    # test async context manager
    async with client as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)

    # test creating with async context manager
    async with raw_client.RawClient() as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)


async def test_replace(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that replace uses PUT to fully replace a thing."""
    route = httpx2_mock.put("https://example.com/customer/123x456").respond(204)

    async with raw_client.RawClient() as client:
        response = await client.replace(
            typename="customer",
            uid="123x456",
            data={"name": "New Name", "email": "new@example.com"},
        )

    assert response.status_code == 204
    assert route.call_count == 1


async def test_bulk_create(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that bulk_create posts newline-delimited JSON."""
    # bubble returns text/plain with one JSON object per line
    mock_response_text = '{"status":"success","id":"1234x5678"}\n{"status":"success","id":"1234x5679"}'
    route = httpx2_mock.post("https://example.com/customer/bulk").respond(
        200, text=mock_response_text, headers={"content-type": "text/plain"}
    )

    async with raw_client.RawClient() as client:
        response = await client.bulk_create(
            typename="customer",
            data=[{"name": "Alice"}, {"name": "Bob"}],
        )

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain"
    assert route.call_count == 1
    # verify it sent newline-delimited JSON
    request_content = route.calls[0].request.content.decode()
    assert request_content == '{"name": "Alice"}\n{"name": "Bob"}'


async def test_bulk_create_parsed_success(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that bulk_create_parsed returns parsed results on success."""
    mock_response_text = '{"status":"success","id":"1234x5678"}\n{"status":"success","id":"1234x5679"}'
    httpx2_mock.post("https://example.com/customer/bulk").respond(
        200, text=mock_response_text, headers={"content-type": "text/plain"}
    )

    async with raw_client.RawClient() as client:
        results = await client.bulk_create_parsed(
            typename="customer",
            data=[{"name": "Alice"}, {"name": "Bob"}],
        )

    assert len(results) == 2
    assert results[0]["status"] == "success"
    assert results[0]["id"] == "1234x5678"
    assert results[0]["message"] is None
    assert results[1]["status"] == "success"
    assert results[1]["id"] == "1234x5679"
    assert results[1]["message"] is None


async def test_bulk_create_parsed_partial_failure(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that bulk_create_parsed returns parsed results on partial failure."""
    mock_response_text = '{"status":"success","id":"1234x5678"}\n{"status":"error","message":"Invalid field value"}'
    httpx2_mock.post("https://example.com/customer/bulk").respond(
        200, text=mock_response_text, headers={"content-type": "text/plain"}
    )

    async with raw_client.RawClient() as client:
        results = await client.bulk_create_parsed(
            typename="customer",
            data=[{"name": "Alice"}, {"name": ""}],
        )

    assert len(results) == 2
    assert results[0]["status"] == "success"
    assert results[0]["id"] == "1234x5678"
    assert results[0]["message"] is None
    assert results[1]["status"] == "error"
    assert results[1]["id"] is None
    assert results[1]["message"] == "Invalid field value"


async def test_find_with_parameters(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that find passes optional parameters correctly."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            cursor=10,
            limit=50,
            sort_field="name",
            descending=True,
            exclude_remaining=True,
        )

    assert route.call_count == 1
    request = route.calls[0].request
    assert "cursor=10" in str(request.url)
    assert "limit=50" in str(request.url)
    assert "sort_field=name" in str(request.url)
    assert "descending=true" in str(request.url)
    assert "exclude_remaining=true" in str(request.url)


async def test_find_serializes_datetime_constraint_value(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify tz-aware datetime constraint values become ISO 8601 strings (Z-suffixed for UTC)."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    modified_after = datetime(2025, 1, 15, 14, 30, 0, tzinfo=UTC)
    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            constraints=[{"key": "Modified Date", "constraint_type": "greater than", "value": modified_after}],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    assert "2025-01-15T14:30:00Z" in constraints_param


async def test_find_serializes_date_constraint_value(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify date constraint values become ISO 8601 strings in the URL."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    cutoff = date(2025, 1, 15)
    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            constraints=[{"key": "birthday", "constraint_type": "less than", "value": cutoff}],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    assert cutoff.isoformat() in constraints_param


async def test_find_serializes_datetimes_in_in_constraint(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify datetime values nested inside an IN constraint list are converted."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    a = datetime(2025, 1, 1, tzinfo=UTC)
    b = datetime(2025, 6, 1, tzinfo=UTC)
    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            constraints=[{"key": "Created Date", "constraint_type": "in", "value": [a, b]}],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    assert "2025-01-01T00:00:00Z" in constraints_param
    assert "2025-06-01T00:00:00Z" in constraints_param


async def test_find_serializes_decimal_constraint_value(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify Decimal constraint values become strings in the URL (matches pydantic body path)."""
    route = httpx2_mock.get("https://example.com/product").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    price = Decimal("9.99")
    async with raw_client.RawClient() as client:
        await client.find(
            typename="product",
            constraints=[{"key": "price", "constraint_type": "greater than", "value": price}],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    assert '"9.99"' in constraints_param


async def test_find_serializes_uuid_constraint_value(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify UUID constraint values become canonical hex strings in the URL."""
    route = httpx2_mock.get("https://example.com/event").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    external_id = UUID("12345678-1234-5678-1234-567812345678")
    async with raw_client.RawClient() as client:
        await client.find(
            typename="event",
            constraints=[{"key": "external_id", "constraint_type": "equals", "value": external_id}],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    assert str(external_id) in constraints_param


async def test_find_serializes_enum_constraint_value(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify plain Enum members serialize via their .value (IntEnum/StrEnum already serialize natively)."""

    class Status(Enum):
        ACTIVE = "active"

    class Priority(IntEnum):
        HIGH = 1

    route = httpx2_mock.get("https://example.com/order").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    async with raw_client.RawClient() as client:
        await client.find(
            typename="order",
            constraints=[
                {"key": "status", "constraint_type": "equals", "value": Status.ACTIVE},
                {"key": "priority", "constraint_type": "equals", "value": Priority.HIGH},
            ],
        )

    constraints_param = urllib.parse.parse_qs(route.calls[0].request.url.query.decode())["constraints"][0]
    parsed = json.loads(constraints_param)
    assert parsed[0]["value"] == "active"
    assert parsed[1]["value"] == 1


async def test_find_rejects_unsupported_constraint_value_type(configured_client: None) -> None:
    """Verify pydantic raises a clear serialization error for genuinely unsupported types."""

    class Unsupported:
        pass

    async with raw_client.RawClient() as client:
        with pytest.raises(PydanticSerializationError, match="Unable to serialize"):
            await client.find(
                typename="customer",
                constraints=[{"key": "x", "constraint_type": "equals", "value": Unsupported()}],
            )


async def test_find_with_additional_sort_fields(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that find passes additional_sort_fields correctly."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )

    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            additional_sort_fields=[{"sort_field": "age", "descending": False}],
        )

    assert route.call_count == 1
    request = route.calls[0].request
    assert "additional_sort_fields" in str(request.url)


async def test_count(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that count returns total from count + remaining."""
    httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [], "count": 5, "remaining": 95}}
    )

    async with raw_client.RawClient() as client:
        total = await client.count(typename="customer")

    assert total == 100


async def test_find_page_single_page(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test find_page returns a PageResult with items and envelope metadata."""
    httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={
            "response": {
                "cursor": 0,
                "results": [{"_id": "a1"}, {"_id": "a2"}, {"_id": "a3"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )

    async with raw_client.RawClient() as client:
        page = await client.find_page(typename="customer")

    assert len(page.items) == 3
    assert page.cursor == 0
    assert page.remaining == 0
    assert page.total == 3
    assert page.has_more is False


async def test_find_page_middle_page_computes_total(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test find_page total is cursor + len(items) + remaining."""
    httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={
            "response": {
                "cursor": 100,
                "results": [{"_id": f"a{i}"} for i in range(50)],
                "count": 50,
                "remaining": 400,
            }
        },
    )

    async with raw_client.RawClient() as client:
        page = await client.find_page(typename="customer", cursor=100, limit=50)

    assert len(page.items) == 50
    assert page.cursor == 100
    assert page.remaining == 400
    assert page.total == 550
    assert page.has_more is True


async def test_find_page_last_partial_page(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test find_page on a last partial page: has_more is False, total correct."""
    httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={
            "response": {
                "cursor": 97,
                "results": [{"_id": "a1"}, {"_id": "a2"}, {"_id": "a3"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )

    async with raw_client.RawClient() as client:
        page = await client.find_page(typename="customer", cursor=97, limit=50)

    assert len(page.items) == 3
    assert page.cursor == 97
    assert page.total == 100
    assert page.has_more is False


async def test_find_page_empty_result(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test find_page with zero matches returns empty PageResult."""
    httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={"response": {"cursor": 0, "results": [], "count": 0, "remaining": 0}},
    )

    async with raw_client.RawClient() as client:
        page = await client.find_page(typename="customer")

    assert page.items == []
    assert page.cursor == 0
    assert page.remaining == 0
    assert page.total == 0
    assert page.has_more is False


async def test_find_page_reads_cursor_from_server_envelope(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page uses the cursor reported by Bubble, not the echoed request.

    In practice Bubble echoes the requested cursor, but find_page is
    documented to reflect server state. If Bubble ever normalizes the
    value (e.g. a negative cursor to 0), PageResult.cursor should
    reflect that normalization, not hide it.
    """
    # server returns a cursor different from what we requested
    httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={
            "response": {
                "cursor": 7,  # server-normalized value (simulated)
                "results": [{"_id": "a1"}],
                "count": 1,
                "remaining": 0,
            }
        },
    )

    async with raw_client.RawClient() as client:
        page = await client.find_page(typename="customer", cursor=42)

    assert page.cursor == 7  # server value, not the requested 42


async def test_find_page_forwards_params(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test find_page forwards cursor, limit, sort, and constraints; never sends exclude_remaining."""
    route = httpx2_mock.get("https://example.com/customer").respond(
        200,
        json={"response": {"cursor": 50, "results": [], "count": 0, "remaining": 0}},
    )

    async with raw_client.RawClient() as client:
        await client.find_page(
            typename="customer",
            constraints=[{"key": "status", "constraint_type": "equals", "value": "active"}],
            cursor=50,
            limit=25,
            sort_field="Created Date",
            descending=True,
        )

    assert route.call_count == 1
    request_url = str(route.calls[0].request.url)
    assert "cursor=50" in request_url
    assert "limit=25" in request_url
    assert "descending=true" in request_url
    assert "constraints" in request_url
    # find_page's contract is to return envelope metadata, so exclude_remaining
    # must never be sent.
    assert "exclude_remaining" not in request_url


async def test_exists_by_uid_found(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test exists returns True when record found by uid."""
    httpx2_mock.get("https://example.com/customer/123x456").respond(200, json={"response": {"_id": "123x456"}})

    async with raw_client.RawClient() as client:
        result = await client.exists(typename="customer", uid="123x456")

    assert result is True


async def test_exists_by_uid_not_found(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test exists returns False when record not found by uid."""
    httpx2_mock.get("https://example.com/customer/123x456").respond(404, json={"status": "NOT_FOUND"})

    async with raw_client.RawClient() as client:
        result = await client.exists(typename="customer", uid="123x456")

    assert result is False


async def test_exists_by_uid_error_reraises(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test exists re-raises non-404 HTTP errors."""
    httpx2_mock.get("https://example.com/customer/123x456").respond(500, json={"error": "server error"})

    async with raw_client.RawClient() as client:
        with pytest.raises(BubbleAPIError) as exc_info:
            await client.exists(typename="customer", uid="123x456")

    assert exc_info.value.status_code == 500


async def test_exists_by_constraints(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test exists with constraints uses find."""
    httpx2_mock.get("https://example.com/customer").respond(
        200, json={"response": {"results": [{"_id": "1x1"}], "count": 1, "remaining": 0}}
    )

    async with raw_client.RawClient() as client:
        result = await client.exists(
            typename="customer",
            constraints=[{"key": "email", "constraint_type": "equals", "value": "test@example.com"}],
        )

    assert result is True


async def test_exists_uid_and_constraints_raises(configured_client: None) -> None:
    """Test exists raises when both uid and constraints provided."""
    async with raw_client.RawClient() as client:
        with pytest.raises(ValueError, match="Cannot specify both"):
            await client.exists(
                typename="customer",
                uid="123x456",
                constraints=[{"key": "x", "constraint_type": "equals"}],
            )
