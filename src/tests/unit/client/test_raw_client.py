import httpx
import pytest
import respx

from bubble_data_api_client import BubbleAPIError
from bubble_data_api_client.client import raw_client


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


@respx.mock
async def test_replace(configured_client: None) -> None:
    """Test that replace uses PUT to fully replace a thing."""
    route = respx.put("https://example.com/customer/123x456").mock(return_value=httpx.Response(204))

    async with raw_client.RawClient() as client:
        response = await client.replace(
            typename="customer",
            uid="123x456",
            data={"name": "New Name", "email": "new@example.com"},
        )

    assert response.status_code == 204
    assert route.call_count == 1


@respx.mock
async def test_bulk_create(configured_client: None) -> None:
    """Test that bulk_create posts newline-delimited JSON."""
    route = respx.post("https://example.com/customer/bulk").mock(
        return_value=httpx.Response(200, json={"status": "success", "count": 2})
    )

    async with raw_client.RawClient() as client:
        response = await client.bulk_create(
            typename="customer",
            data=[{"name": "Alice"}, {"name": "Bob"}],
        )

    assert response.status_code == 200
    assert route.call_count == 1
    # verify it sent newline-delimited JSON
    request_content = route.calls[0].request.content.decode()
    assert request_content == '{"name": "Alice"}\n{"name": "Bob"}'


@respx.mock
async def test_find_with_parameters(configured_client: None) -> None:
    """Test that find passes optional parameters correctly."""
    route = respx.get("https://example.com/customer").mock(
        return_value=httpx.Response(200, json={"response": {"results": [], "count": 0, "remaining": 0}})
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


@respx.mock
async def test_find_with_additional_sort_fields(configured_client: None) -> None:
    """Test that find passes additional_sort_fields correctly."""
    route = respx.get("https://example.com/customer").mock(
        return_value=httpx.Response(200, json={"response": {"results": [], "count": 0, "remaining": 0}})
    )

    async with raw_client.RawClient() as client:
        await client.find(
            typename="customer",
            additional_sort_fields=[{"sort_field": "age", "descending": False}],
        )

    assert route.call_count == 1
    request = route.calls[0].request
    assert "additional_sort_fields" in str(request.url)


@respx.mock
async def test_count(configured_client: None) -> None:
    """Test that count returns total from count + remaining."""
    respx.get("https://example.com/customer").mock(
        return_value=httpx.Response(200, json={"response": {"results": [], "count": 5, "remaining": 95}})
    )

    async with raw_client.RawClient() as client:
        total = await client.count(typename="customer")

    assert total == 100


@respx.mock
async def test_exists_by_uid_found(configured_client: None) -> None:
    """Test exists returns True when record found by uid."""
    respx.get("https://example.com/customer/123x456").mock(
        return_value=httpx.Response(200, json={"response": {"_id": "123x456"}})
    )

    async with raw_client.RawClient() as client:
        result = await client.exists(typename="customer", uid="123x456")

    assert result is True


@respx.mock
async def test_exists_by_uid_not_found(configured_client: None) -> None:
    """Test exists returns False when record not found by uid."""
    respx.get("https://example.com/customer/123x456").mock(
        return_value=httpx.Response(404, json={"status": "NOT_FOUND"})
    )

    async with raw_client.RawClient() as client:
        result = await client.exists(typename="customer", uid="123x456")

    assert result is False


@respx.mock
async def test_exists_by_uid_error_reraises(configured_client: None) -> None:
    """Test exists re-raises non-404 HTTP errors."""
    respx.get("https://example.com/customer/123x456").mock(
        return_value=httpx.Response(500, json={"error": "server error"})
    )

    async with raw_client.RawClient() as client:
        with pytest.raises(BubbleAPIError) as exc_info:
            await client.exists(typename="customer", uid="123x456")

    assert exc_info.value.status_code == 500


@respx.mock
async def test_exists_by_constraints(configured_client: None) -> None:
    """Test exists with constraints uses find."""
    respx.get("https://example.com/customer").mock(
        return_value=httpx.Response(200, json={"response": {"results": [{"_id": "1x1"}], "count": 1, "remaining": 0}})
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
            await client.exists(typename="customer", uid="123x456", constraints=[{"key": "x"}])
