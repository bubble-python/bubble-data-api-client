import json
from datetime import UTC, datetime

import httpx
import pytest
import respx
from pydantic import Field

from bubble_data_api_client.client.orm import BubbleModel
from bubble_data_api_client.exceptions import BubbleAPIError, UnknownFieldError


def test_model_instantiation():
    """Tests that the Pydantic model can be instantiated."""

    class User(BubbleModel, typename="user"):
        name: str

    # instantiate the model, no client is needed
    user = User(name="testuser", _id="12345")

    assert user.uid == "12345"
    assert user.name == "testuser"


@respx.mock
async def test_save_uses_field_aliases(configured_client: None) -> None:
    """Verify save() sends Bubble aliases, not Python field names."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")

    order = Order(**{"Buying company": "Acme Corp", "_id": "abc123"})

    route = respx.patch("https://example.com/order/abc123").mock(return_value=httpx.Response(204))

    await order.save()

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"Buying company": "Acme Corp"}


@respx.mock
async def test_update_single_field(configured_client: None) -> None:
    """Verify update() sends only the specified field."""

    class User(BubbleModel, typename="user"):
        name: str
        email: str

    route = respx.patch("https://example.com/user/abc123").mock(return_value=httpx.Response(204))

    await User.update(uid="abc123", name="New Name")

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"name": "New Name"}


@respx.mock
async def test_update_translates_field_aliases(configured_client: None) -> None:
    """Verify update() translates Python field names to Bubble aliases."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")
        status: str

    route = respx.patch("https://example.com/order/xyz789").mock(return_value=httpx.Response(204))

    await Order.update(uid="xyz789", company="Acme Corp", status="active")

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"Buying company": "Acme Corp", "status": "active"}


async def test_update_raises_for_unknown_field() -> None:
    """Verify update() raises UnknownFieldError for fields not in the model."""

    class User(BubbleModel, typename="user"):
        name: str

    with pytest.raises(UnknownFieldError, match="unknown field: nonexistent"):
        await User.update(uid="abc123", nonexistent="value")


@respx.mock
async def test_create_translates_field_aliases(configured_client: None) -> None:
    """Verify create() translates Python field names to Bubble aliases."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")
        status: str

    route = respx.post("https://example.com/order").mock(
        return_value=httpx.Response(200, json={"status": "success", "id": "new123"})
    )

    order = await Order.create(company="Acme Corp", status="pending")

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"Buying company": "Acme Corp", "status": "pending"}
    assert order.company == "Acme Corp"
    assert order.status == "pending"
    assert order.uid == "new123"


async def test_create_raises_for_unknown_field() -> None:
    """Verify create() raises UnknownFieldError for fields not in the model."""

    class User(BubbleModel, typename="user"):
        name: str

    with pytest.raises(UnknownFieldError, match="unknown field: nonexistent"):
        await User.create(name="test", nonexistent="value")


@respx.mock
async def test_create_or_update_translates_match_aliases(configured_client: None) -> None:
    """Verify create_or_update() translates match field names to Bubble aliases."""
    from bubble_data_api_client.types import OnMultiple

    class Order(BubbleModel, typename="order"):
        external_id: str = Field(alias="External ID")
        company: str = Field(alias="Buying company")

    # mock find returning no results (will create)
    find_route = respx.get("https://example.com/order").mock(
        return_value=httpx.Response(200, json={"response": {"results": [], "count": 0, "remaining": 0}})
    )
    # mock create
    create_route = respx.post("https://example.com/order").mock(
        return_value=httpx.Response(200, json={"status": "success", "id": "new123"})
    )

    _order, created = await Order.create_or_update(
        match={"external_id": "ext-001"},
        create_data={"company": "Acme Corp"},
        on_multiple=OnMultiple.ERROR,
    )

    assert created is True
    assert find_route.call_count == 1
    # verify find used aliased field name in constraint
    find_request_url = str(find_route.calls[0].request.url)
    assert "External%20ID" in find_request_url or "External+ID" in find_request_url

    assert create_route.call_count == 1
    request_body = json.loads(create_route.calls[0].request.content)
    assert request_body == {"External ID": "ext-001", "Buying company": "Acme Corp"}


@respx.mock
async def test_create_or_update_translates_data_aliases(configured_client: None) -> None:
    """Verify create_or_update() translates data field names to Bubble aliases."""
    from bubble_data_api_client.types import OnMultiple

    class Order(BubbleModel, typename="order"):
        external_id: str = Field(alias="External ID")
        company: str = Field(alias="Buying company")

    # mock find returning one result (will update)
    respx.get("https://example.com/order").mock(
        return_value=httpx.Response(
            200, json={"response": {"results": [{"_id": "existing123"}], "count": 1, "remaining": 0}}
        )
    )
    # mock update
    update_route = respx.patch("https://example.com/order/existing123").mock(return_value=httpx.Response(204))

    _order, created = await Order.create_or_update(
        match={"external_id": "ext-001"},
        update_data={"company": "Updated Corp"},
        on_multiple=OnMultiple.ERROR,
    )

    assert created is False
    assert update_route.call_count == 1
    request_body = json.loads(update_route.calls[0].request.content)
    assert request_body == {"Buying company": "Updated Corp"}


async def test_create_or_update_raises_for_unknown_match_field() -> None:
    """Verify create_or_update() raises UnknownFieldError for unknown match fields."""
    from bubble_data_api_client.types import OnMultiple

    class User(BubbleModel, typename="user"):
        name: str

    with pytest.raises(UnknownFieldError, match="unknown field: nonexistent"):
        await User.create_or_update(
            match={"nonexistent": "value"},
            update_data={"name": "test"},
            on_multiple=OnMultiple.ERROR,
        )


async def test_create_or_update_raises_for_unknown_data_field() -> None:
    """Verify create_or_update() raises UnknownFieldError for unknown data fields."""
    from bubble_data_api_client.types import OnMultiple

    class User(BubbleModel, typename="user"):
        name: str

    with pytest.raises(UnknownFieldError, match="unknown field: nonexistent"):
        await User.create_or_update(
            match={"name": "test"},
            update_data={"nonexistent": "value"},
            on_multiple=OnMultiple.ERROR,
        )


@respx.mock
async def test_find_iter_single_page(configured_client: None) -> None:
    """Verify find_iter yields all items from a single page."""

    class User(BubbleModel, typename="user"):
        name: str

    respx.get("https://example.com/user").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "results": [
                        {"_id": "1", "name": "Alice"},
                        {"_id": "2", "name": "Bob"},
                    ],
                    "count": 2,
                    "remaining": 0,
                }
            },
        )
    )

    users = [user async for user in User.find_iter()]

    assert len(users) == 2
    assert users[0].uid == "1"
    assert users[0].name == "Alice"
    assert users[1].uid == "2"
    assert users[1].name == "Bob"


@respx.mock
async def test_find_iter_multiple_pages(configured_client: None) -> None:
    """Verify find_iter fetches all pages and yields items from each."""

    class User(BubbleModel, typename="user"):
        name: str

    route = respx.get("https://example.com/user")
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "1", "name": "Alice"}],
                    "count": 1,
                    "remaining": 2,
                }
            },
        ),
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "2", "name": "Bob"}],
                    "count": 1,
                    "remaining": 1,
                }
            },
        ),
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "3", "name": "Charlie"}],
                    "count": 1,
                    "remaining": 0,
                }
            },
        ),
    ]

    users = [user async for user in User.find_iter(page_size=1)]

    assert len(users) == 3
    assert [u.name for u in users] == ["Alice", "Bob", "Charlie"]
    assert route.call_count == 3


@respx.mock
async def test_find_iter_empty_results(configured_client: None) -> None:
    """Verify find_iter handles empty results."""

    class User(BubbleModel, typename="user"):
        name: str

    respx.get("https://example.com/user").mock(
        return_value=httpx.Response(
            200,
            json={"response": {"results": [], "count": 0, "remaining": 0}},
        )
    )

    users = [user async for user in User.find_iter()]

    assert users == []


@respx.mock
async def test_find_all_returns_list(configured_client: None) -> None:
    """Verify find_all returns all items as a list."""

    class User(BubbleModel, typename="user"):
        name: str

    route = respx.get("https://example.com/user")
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "1", "name": "Alice"}],
                    "count": 1,
                    "remaining": 1,
                }
            },
        ),
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "2", "name": "Bob"}],
                    "count": 1,
                    "remaining": 0,
                }
            },
        ),
    ]

    users = await User.find_all(page_size=1)

    assert isinstance(users, list)
    assert len(users) == 2
    assert users[0].name == "Alice"
    assert users[1].name == "Bob"


@respx.mock
async def test_refresh_updates_instance_in_place(configured_client: None) -> None:
    """Verify refresh() fetches data and updates the instance in place."""

    class User(BubbleModel, typename="user"):
        name: str
        email: str | None = None

    user = User(_id="abc123", name="Old Name", email=None)

    respx.get("https://example.com/user/abc123").mock(
        return_value=httpx.Response(
            200,
            json={"response": {"_id": "abc123", "name": "New Name", "email": "new@example.com"}},
        )
    )

    result = await user.refresh()

    # verify instance was updated in place
    assert user.name == "New Name"
    assert user.email == "new@example.com"
    # verify returns self for chaining
    assert result is user


@respx.mock
async def test_refresh_updates_server_computed_fields(configured_client: None) -> None:
    """Verify refresh() populates server-computed fields like modified_date."""

    class User(BubbleModel, typename="user"):
        name: str

    user = User(_id="abc123", name="Test")
    assert user.modified_date is None

    respx.get("https://example.com/user/abc123").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "_id": "abc123",
                    "name": "Test",
                    "Created Date": "2024-01-15T10:30:00.000Z",
                    "Modified Date": "2024-01-16T14:20:00.000Z",
                }
            },
        )
    )

    await user.refresh()

    assert user.created_date == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    assert user.modified_date == datetime(2024, 1, 16, 14, 20, 0, tzinfo=UTC)


@respx.mock
async def test_refresh_raises_on_not_found(configured_client: None) -> None:
    """Verify refresh() raises BubbleAPIError when record no longer exists."""

    class User(BubbleModel, typename="user"):
        name: str

    user = User(_id="deleted123", name="Ghost")

    respx.get("https://example.com/user/deleted123").mock(
        return_value=httpx.Response(
            404,
            json={"body": {"status": "NOT_FOUND", "message": "Thing not found"}},
        )
    )

    with pytest.raises(BubbleAPIError) as exc_info:
        await user.refresh()

    assert exc_info.value.status_code == 404
