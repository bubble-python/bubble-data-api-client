from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

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


async def test_create_or_update_creates_when_no_match(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that create_or_update creates a new thing when no match is found."""
    # mock find returning no results
    find_route = httpx2_mock.get("https://test.example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )
    # mock create returning new id
    create_route = httpx2_mock.post("https://test.example.com/customer").respond(
        200, json={"status": "success", "id": "123x456"}
    )

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            create_data={"name": "John"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is True
    assert result["uids"] == ["123x456"]
    assert find_route.call_count == 1
    assert create_route.call_count == 1


async def test_create_or_update_with_both_create_and_update_data_creates(
    configured_client: None, httpx2_mock: respx.Router
) -> None:
    """Test that create_data is used when creating, not update_data."""
    import json

    httpx2_mock.get("https://test.example.com/customer").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )
    create_route = httpx2_mock.post("https://test.example.com/customer").respond(
        200, json={"status": "success", "id": "new123"}
    )

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            create_data={"status": "new", "created_by": "system"},
            update_data={"status": "active", "last_seen": "2024-01-01"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is True
    assert result["uids"] == ["new123"]
    # verify create was called with match + create_data (not update_data)
    request_body = json.loads(create_route.calls[0].request.content)
    assert request_body == {"external_id": "abc", "status": "new", "created_by": "system"}


async def test_create_or_update_with_both_create_and_update_data_updates(
    configured_client: None, httpx2_mock: respx.Router
) -> None:
    """Test that update_data is used when updating, not create_data."""
    import json

    httpx2_mock.get("https://test.example.com/customer").respond(
        200, json={"response": {"results": [{"_id": "existing123"}], "count": 1, "remaining": 0}}
    )
    update_route = httpx2_mock.patch("https://test.example.com/customer/existing123").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            create_data={"status": "new", "created_by": "system"},
            update_data={"status": "active", "last_seen": "2024-01-01"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is False
    assert result["uids"] == ["existing123"]
    # verify update was called with update_data (not create_data)
    request_body = json.loads(update_route.calls[0].request.content)
    assert request_body == {"status": "active", "last_seen": "2024-01-01"}


async def test_create_or_update_with_only_create_data_skips_update(
    configured_client: None, httpx2_mock: respx.Router
) -> None:
    """Test that when only create_data is provided, updates are skipped."""
    find_route = httpx2_mock.get("https://test.example.com/customer").respond(
        200, json={"response": {"results": [{"_id": "existing123"}], "count": 1, "remaining": 0}}
    )
    # no update route - should not be called

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            create_data={"status": "new"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is False
    assert result["uids"] == ["existing123"]
    assert find_route.call_count == 1
    # no PATCH call should have been made


async def test_create_or_update_updates_when_single_match(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that create_or_update updates when exactly one match is found."""
    # mock find returning one result
    find_route = httpx2_mock.get("https://test.example.com/customer").respond(
        200, json={"response": {"results": [{"_id": "existing123", "name": "Old"}], "count": 1, "remaining": 0}}
    )
    # mock update
    update_route = httpx2_mock.patch("https://test.example.com/customer/existing123").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.ERROR,
        )

    assert result["created"] is False
    assert result["uids"] == ["existing123"]
    assert find_route.call_count == 1
    assert update_route.call_count == 1


async def test_create_or_update_error_on_multiple_matches(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that create_or_update raises error when multiple matches with ERROR strategy."""
    # mock find returning multiple results
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "id1"}, {"_id": "id2"}],
                "count": 2,
                "remaining": 0,
            }
        },
    )

    async with RawClient() as client:
        with pytest.raises(MultipleMatchesError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                update_data={"name": "John"},
                on_multiple=OnMultiple.ERROR,
            )

    assert exc_info.value.count == 2
    assert exc_info.value.typename == "customer"


async def test_create_or_update_update_first(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that UPDATE_FIRST updates only the first match."""
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "id1"}, {"_id": "id2"}],
                "count": 2,
                "remaining": 0,
            }
        },
    )
    update_route = httpx2_mock.patch("https://test.example.com/customer/id1").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.UPDATE_FIRST,
        )

    assert result["created"] is False
    assert result["uids"] == ["id1"]
    assert update_route.call_count == 1


async def test_create_or_update_update_all(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that UPDATE_ALL updates all matches."""
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "id1"}, {"_id": "id2"}, {"_id": "id3"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    update_route1 = httpx2_mock.patch("https://test.example.com/customer/id1").respond(204)
    update_route2 = httpx2_mock.patch("https://test.example.com/customer/id2").respond(204)
    update_route3 = httpx2_mock.patch("https://test.example.com/customer/id3").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.UPDATE_ALL,
        )

    assert result["created"] is False
    assert result["uids"] == ["id1", "id2", "id3"]
    assert update_route1.call_count == 1
    assert update_route2.call_count == 1
    assert update_route3.call_count == 1


async def test_create_or_update_dedupe_oldest_created(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that DEDUPE_OLDEST_CREATED keeps oldest by created date and deletes others."""
    # find returns results sorted by Created Date ascending (oldest first)
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "oldest"}, {"_id": "newer"}, {"_id": "newest"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    delete_newer = httpx2_mock.delete("https://test.example.com/customer/newer").respond(204)
    delete_newest = httpx2_mock.delete("https://test.example.com/customer/newest").respond(204)
    update_oldest = httpx2_mock.patch("https://test.example.com/customer/oldest").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_OLDEST_CREATED,
        )

    assert result["created"] is False
    assert result["uids"] == ["oldest"]
    assert delete_newer.call_count == 1
    assert delete_newest.call_count == 1
    assert update_oldest.call_count == 1


async def test_create_or_update_dedupe_newest_created(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that DEDUPE_NEWEST_CREATED keeps newest by created date and deletes others."""
    # find returns results sorted by Created Date descending (newest first)
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "newest"}, {"_id": "newer"}, {"_id": "oldest"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    delete_newer = httpx2_mock.delete("https://test.example.com/customer/newer").respond(204)
    delete_oldest = httpx2_mock.delete("https://test.example.com/customer/oldest").respond(204)
    update_newest = httpx2_mock.patch("https://test.example.com/customer/newest").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_NEWEST_CREATED,
        )

    assert result["created"] is False
    assert result["uids"] == ["newest"]
    assert delete_newer.call_count == 1
    assert delete_oldest.call_count == 1
    assert update_newest.call_count == 1


async def test_create_or_update_dedupe_oldest_modified(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that DEDUPE_OLDEST_MODIFIED keeps oldest by modified date and deletes others."""
    # find returns results sorted by Modified Date ascending (least recently modified first)
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "oldest_mod"}, {"_id": "newer_mod"}, {"_id": "newest_mod"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    delete_newer = httpx2_mock.delete("https://test.example.com/customer/newer_mod").respond(204)
    delete_newest = httpx2_mock.delete("https://test.example.com/customer/newest_mod").respond(204)
    update_oldest = httpx2_mock.patch("https://test.example.com/customer/oldest_mod").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
            on_multiple=OnMultiple.DEDUPE_OLDEST_MODIFIED,
        )

    assert result["created"] is False
    assert result["uids"] == ["oldest_mod"]
    assert delete_newer.call_count == 1
    assert delete_newest.call_count == 1
    assert update_oldest.call_count == 1


async def test_create_or_update_dedupe_newest_modified(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that DEDUPE_NEWEST_MODIFIED keeps newest by modified date and deletes others."""
    # find returns results sorted by Modified Date descending (most recently modified first)
    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "newest_mod"}, {"_id": "newer_mod"}, {"_id": "oldest_mod"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    delete_newer = httpx2_mock.delete("https://test.example.com/customer/newer_mod").respond(204)
    delete_oldest = httpx2_mock.delete("https://test.example.com/customer/oldest_mod").respond(204)
    update_newest = httpx2_mock.patch("https://test.example.com/customer/newest_mod").respond(204)

    async with RawClient() as client:
        result = await client.create_or_update(
            typename="customer",
            match={"external_id": "abc"},
            update_data={"name": "John"},
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
                update_data={"name": "John"},
                on_multiple="invalid",  # type: ignore[arg-type]
            )


async def test_create_or_update_empty_match_raises(configured_client: None) -> None:
    """Test that empty match dict raises ValueError."""
    async with RawClient() as client:
        with pytest.raises(ValueError, match="match cannot be empty"):
            await client.create_or_update(
                typename="customer",
                match={},
                update_data={"name": "John"},
                on_multiple=OnMultiple.ERROR,
            )


async def test_create_or_update_no_data_raises(configured_client: None) -> None:
    """Test that no data provided raises ValueError."""
    async with RawClient() as client:
        with pytest.raises(ValueError, match="at least one of create_data or update_data must be provided"):
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                on_multiple=OnMultiple.ERROR,
            )


async def test_create_or_update_update_all_partial_failure(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Test that UPDATE_ALL reports partial failures via PartialFailureError."""
    from bubble_data_api_client.exceptions import PartialFailureError

    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "id1"}, {"_id": "id2"}, {"_id": "id3"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    # id1 succeeds, id2 fails, id3 succeeds
    httpx2_mock.patch("https://test.example.com/customer/id1").respond(204)
    httpx2_mock.patch("https://test.example.com/customer/id2").respond(500)
    httpx2_mock.patch("https://test.example.com/customer/id3").respond(204)

    async with RawClient() as client:
        with pytest.raises(PartialFailureError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                update_data={"name": "John"},
                on_multiple=OnMultiple.UPDATE_ALL,
            )

    error = exc_info.value
    assert error.operation == "update"
    assert set(error.succeeded) == {"id1", "id3"}
    assert error.failed_uids == ["id2"]
    assert len(error.exceptions) == 1


async def test_create_or_update_dedupe_partial_delete_failure(
    configured_client: None, httpx2_mock: respx.Router
) -> None:
    """Test that DEDUPE reports partial delete failures via PartialFailureError."""
    from bubble_data_api_client.exceptions import PartialFailureError

    httpx2_mock.get("https://test.example.com/customer").respond(
        200,
        json={
            "response": {
                "results": [{"_id": "oldest"}, {"_id": "newer"}, {"_id": "newest"}],
                "count": 3,
                "remaining": 0,
            }
        },
    )
    # update succeeds
    httpx2_mock.patch("https://test.example.com/customer/oldest").respond(204)
    # one delete succeeds, one fails
    httpx2_mock.delete("https://test.example.com/customer/newer").respond(204)
    httpx2_mock.delete("https://test.example.com/customer/newest").respond(500)

    async with RawClient() as client:
        with pytest.raises(PartialFailureError) as exc_info:
            await client.create_or_update(
                typename="customer",
                match={"external_id": "abc"},
                update_data={"name": "John"},
                on_multiple=OnMultiple.DEDUPE_OLDEST_CREATED,
            )

    error = exc_info.value
    assert error.operation == "delete"
    assert error.succeeded == ["newer"]
    assert error.failed_uids == ["newest"]
    assert len(error.exceptions) == 1
