from collections.abc import AsyncGenerator

import httpx
import pytest
import respx

from bubble_data_api_client import configure
from bubble_data_api_client.client.raw_client import RawClient
from bubble_data_api_client.exceptions import InvalidOnMultipleError, MultipleMatchesError
from bubble_data_api_client.pool import close_clients
from bubble_data_api_client.types import OnMultiple


@pytest.fixture
async def clean_client_pool() -> AsyncGenerator[None]:
    """Ensure client pool is clean before and after each test."""
    await close_clients()
    yield
    await close_clients()


@pytest.fixture
def configured_client(clean_client_pool: None) -> None:
    """Configure the client for testing."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=None,
    )


@respx.mock
async def test_create_or_update_creates_when_no_match(configured_client: None) -> None:
    """Test that create_or_update creates a new thing when no match is found."""
    # mock find returning no results
    find_route = respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(200, json={"response": {"results": [], "count": 0, "remaining": 0}})
    )
    # mock create returning new id
    create_route = respx.post("https://test.example.com/customer").mock(
        return_value=httpx.Response(200, json={"status": "success", "id": "123x456"})
    )

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is True
    assert result["uids"] == ["123x456"]
    assert find_route.call_count == 1
    assert create_route.call_count == 1


@respx.mock
async def test_create_or_update_updates_when_single_match(configured_client: None) -> None:
    """Test that create_or_update updates when exactly one match is found."""
    # mock find returning one result
    find_route = respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200, json={"response": {"results": [{"_id": "existing123", "name": "Old"}], "count": 1, "remaining": 0}}
        )
    )
    # mock update
    update_route = respx.patch("https://test.example.com/customer/existing123").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is False
    assert result["uids"] == ["existing123"]
    assert find_route.call_count == 1
    assert update_route.call_count == 1


@respx.mock
async def test_create_or_update_error_on_multiple_matches(configured_client: None) -> None:
    """Test that create_or_update raises error when multiple matches with ERROR strategy."""
    # mock find returning multiple results
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "id1"}, {"_id": "id2"}],
                    "count": 2,
                    "remaining": 0,
                }
            },
        )
    )

    async with RawClient() as client:
        with pytest.raises(MultipleMatchesError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                data={"name": "John"},
                on_multiple=OnMultiple.ERROR,
            )

    assert exc_info.value.count == 2
    assert exc_info.value.typename == "customer"


@respx.mock
async def test_create_or_update_update_first(configured_client: None) -> None:
    """Test that UPDATE_FIRST updates only the first match."""
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "id1"}, {"_id": "id2"}],
                    "count": 2,
                    "remaining": 0,
                }
            },
        )
    )
    update_route = respx.patch("https://test.example.com/customer/id1").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.UPDATE_FIRST,
        )

    assert result["created"] is False
    assert result["uids"] == ["id1"]
    assert update_route.call_count == 1


@respx.mock
async def test_create_or_update_update_all(configured_client: None) -> None:
    """Test that UPDATE_ALL updates all matches."""
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "id1"}, {"_id": "id2"}, {"_id": "id3"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    update_route1 = respx.patch("https://test.example.com/customer/id1").mock(return_value=httpx.Response(204))
    update_route2 = respx.patch("https://test.example.com/customer/id2").mock(return_value=httpx.Response(204))
    update_route3 = respx.patch("https://test.example.com/customer/id3").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.UPDATE_ALL,
        )

    assert result["created"] is False
    assert result["uids"] == ["id1", "id2", "id3"]
    assert update_route1.call_count == 1
    assert update_route2.call_count == 1
    assert update_route3.call_count == 1


@respx.mock
async def test_create_or_update_dedupe_oldest_created(configured_client: None) -> None:
    """Test that DEDUPE_OLDEST_CREATED keeps oldest by created date and deletes others."""
    # find returns results sorted by Created Date ascending (oldest first)
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "oldest"}, {"_id": "newer"}, {"_id": "newest"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    delete_newer = respx.delete("https://test.example.com/customer/newer").mock(return_value=httpx.Response(204))
    delete_newest = respx.delete("https://test.example.com/customer/newest").mock(return_value=httpx.Response(204))
    update_oldest = respx.patch("https://test.example.com/customer/oldest").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_OLDEST_CREATED,
        )

    assert result["created"] is False
    assert result["uids"] == ["oldest"]
    assert delete_newer.call_count == 1
    assert delete_newest.call_count == 1
    assert update_oldest.call_count == 1


@respx.mock
async def test_create_or_update_dedupe_newest_created(configured_client: None) -> None:
    """Test that DEDUPE_NEWEST_CREATED keeps newest by created date and deletes others."""
    # find returns results sorted by Created Date descending (newest first)
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "newest"}, {"_id": "newer"}, {"_id": "oldest"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    delete_newer = respx.delete("https://test.example.com/customer/newer").mock(return_value=httpx.Response(204))
    delete_oldest = respx.delete("https://test.example.com/customer/oldest").mock(return_value=httpx.Response(204))
    update_newest = respx.patch("https://test.example.com/customer/newest").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_NEWEST_CREATED,
        )

    assert result["created"] is False
    assert result["uids"] == ["newest"]
    assert delete_newer.call_count == 1
    assert delete_oldest.call_count == 1
    assert update_newest.call_count == 1


@respx.mock
async def test_create_or_update_dedupe_oldest_modified(configured_client: None) -> None:
    """Test that DEDUPE_OLDEST_MODIFIED keeps oldest by modified date and deletes others."""
    # find returns results sorted by Modified Date ascending (least recently modified first)
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "oldest_mod"}, {"_id": "newer_mod"}, {"_id": "newest_mod"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    delete_newer = respx.delete("https://test.example.com/customer/newer_mod").mock(return_value=httpx.Response(204))
    delete_newest = respx.delete("https://test.example.com/customer/newest_mod").mock(return_value=httpx.Response(204))
    update_oldest = respx.patch("https://test.example.com/customer/oldest_mod").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_OLDEST_MODIFIED,
        )

    assert result["created"] is False
    assert result["uids"] == ["oldest_mod"]
    assert delete_newer.call_count == 1
    assert delete_newest.call_count == 1
    assert update_oldest.call_count == 1


@respx.mock
async def test_create_or_update_dedupe_newest_modified(configured_client: None) -> None:
    """Test that DEDUPE_NEWEST_MODIFIED keeps newest by modified date and deletes others."""
    # find returns results sorted by Modified Date descending (most recently modified first)
    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "newest_mod"}, {"_id": "newer_mod"}, {"_id": "oldest_mod"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    delete_newer = respx.delete("https://test.example.com/customer/newer_mod").mock(return_value=httpx.Response(204))
    delete_oldest = respx.delete("https://test.example.com/customer/oldest_mod").mock(return_value=httpx.Response(204))
    update_newest = respx.patch("https://test.example.com/customer/newest_mod").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_NEWEST_MODIFIED,
        )

    assert result["created"] is False
    assert result["uids"] == ["newest_mod"]
    assert delete_newer.call_count == 1
    assert delete_oldest.call_count == 1
    assert update_newest.call_count == 1


async def test_create_or_update_invalid_on_multiple(configured_client: None) -> None:
    """Test that invalid on_multiple raises InvalidOnMultipleError."""
    async with RawClient() as client:
        with pytest.raises(InvalidOnMultipleError):
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                data={"name": "John"},
                on_multiple="invalid",  # type: ignore[arg-type]
            )


async def test_create_or_update_empty_match_raises(configured_client: None) -> None:
    """Test that empty match dict raises ValueError."""
    async with RawClient() as client:
        with pytest.raises(ValueError, match="match cannot be empty"):
            await client.create_or_update(
                typename="customer",
                match={},
                data={"name": "John"},
                on_multiple=OnMultiple.ERROR,
            )


async def test_create_or_update_empty_data_raises(configured_client: None) -> None:
    """Test that empty data dict raises ValueError."""
    async with RawClient() as client:
        with pytest.raises(ValueError, match="data cannot be empty"):
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                data={},
                on_multiple=OnMultiple.ERROR,
            )


@respx.mock
async def test_create_or_update_update_all_partial_failure(configured_client: None) -> None:
    """Test that UPDATE_ALL reports partial failures via PartialFailureError."""
    from bubble_data_api_client.exceptions import PartialFailureError

    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "id1"}, {"_id": "id2"}, {"_id": "id3"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    # id1 succeeds, id2 fails, id3 succeeds
    respx.patch("https://test.example.com/customer/id1").mock(return_value=httpx.Response(204))
    respx.patch("https://test.example.com/customer/id2").mock(return_value=httpx.Response(500))
    respx.patch("https://test.example.com/customer/id3").mock(return_value=httpx.Response(204))

    async with RawClient() as client:
        with pytest.raises(PartialFailureError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                data={"name": "John"},
                on_multiple=OnMultiple.UPDATE_ALL,
            )

    error = exc_info.value
    assert error.operation == "update"
    assert set(error.succeeded) == {"id1", "id3"}
    assert error.failed_uids == ["id2"]
    assert len(error.exceptions) == 1


@respx.mock
async def test_create_or_update_dedupe_partial_delete_failure(configured_client: None) -> None:
    """Test that DEDUPE reports partial delete failures via PartialFailureError."""
    from bubble_data_api_client.exceptions import PartialFailureError

    respx.get("https://test.example.com/customer").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "oldest"}, {"_id": "newer"}, {"_id": "newest"}],
                    "count": 3,
                    "remaining": 0,
                }
            },
        )
    )
    # update succeeds
    respx.patch("https://test.example.com/customer/oldest").mock(return_value=httpx.Response(204))
    # one delete succeeds, one fails
    respx.delete("https://test.example.com/customer/newer").mock(return_value=httpx.Response(204))
    respx.delete("https://test.example.com/customer/newest").mock(return_value=httpx.Response(500))

    async with RawClient() as client:
        with pytest.raises(PartialFailureError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                data={"name": "John"},
                on_multiple=OnMultiple.DEDUPE_OLDEST_CREATED,
            )

    error = exc_info.value
    assert error.operation == "delete"
    assert error.succeeded == ["newer"]
    assert error.failed_uids == ["newest"]
    assert len(error.exceptions) == 1
