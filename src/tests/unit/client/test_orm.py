from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
import pytest
from pydantic import Field

from bubble_data_api_client.client.orm import BubbleModel
from bubble_data_api_client.exceptions import BubbleAPIError, UnknownFieldError

if TYPE_CHECKING:
    import respx


def test_model_instantiation():
    """Tests that the Pydantic model can be instantiated."""

    class User(BubbleModel, typename="user"):
        name: str

    # instantiate the model, no client is needed
    user = User(name="testuser", _id="12345")

    assert user.uid == "12345"
    assert user.name == "testuser"


def test_bubble_field_returns_custom_string_alias() -> None:
    """Subclass-declared string alias is returned verbatim."""

    class User(BubbleModel, typename="user"):
        first_name: str | None = Field(default=None, alias="firstName")

    assert User.bubble_field("first_name") == "firstName"


def test_bubble_field_returns_alias_with_literal_space() -> None:
    """Aliases containing spaces (common in Bubble) round-trip cleanly."""

    class User(BubbleModel, typename="user"):
        main_company: str | None = Field(default=None, alias="main company")

    assert User.bubble_field("main_company") == "main company"


def test_bubble_field_returns_string_for_builtin_fields() -> None:
    """Built-in fields return plain strings, not BubbleField enums.

    Regression: prior to the fix, the base class declared aliases as
    ``Field(alias=BubbleField.ID)`` (an enum), forcing consumers to coerce.
    Aliases are now uniformly ``str``.
    """

    class User(BubbleModel, typename="user"):
        pass

    uid_alias = User.bubble_field("uid")
    assert uid_alias == "_id"
    assert type(uid_alias) is str

    created_alias = User.bubble_field("created_date")
    assert created_alias == "Created Date"
    assert type(created_alias) is str

    assert User.bubble_field("modified_date") == "Modified Date"
    assert User.bubble_field("slug") == "Slug"


def test_bubble_field_falls_back_to_python_name_when_no_alias() -> None:
    """Fields without an explicit alias return the python attribute name.

    Matches Pydantic's ``by_alias=True`` serialization behavior so the helper
    always reflects what's actually sent to Bubble.
    """

    class User(BubbleModel, typename="user"):
        status: str | None = None

    assert User.bubble_field("status") == "status"


def test_bubble_field_raises_unknown_field_error_for_typo() -> None:
    """Typos in the python attribute name fail loudly with the library's typed exception."""

    class User(BubbleModel, typename="user"):
        first_name: str | None = Field(default=None, alias="firstName")

    with pytest.raises(UnknownFieldError, match="unknown field: frist_name"):
        User.bubble_field("frist_name")


def test_bubble_field_works_at_class_level_without_instance() -> None:
    """Helper is a classmethod usable at module import time."""

    class User(BubbleModel, typename="user"):
        first_name: str | None = Field(default=None, alias="firstName")

    sort_map: dict[str, str] = {
        "FIRST_NAME": User.bubble_field("first_name"),
        "CREATED_DATE": User.bubble_field("created_date"),
    }
    assert sort_map == {"FIRST_NAME": "firstName", "CREATED_DATE": "Created Date"}


async def test_save_uses_field_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify save() sends Bubble aliases, not Python field names."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")

    order = Order.model_validate({"Buying company": "Acme Corp", "_id": "abc123"})

    route = httpx2_mock.patch("https://example.com/order/abc123").respond(204)

    await order.save()

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"Buying company": "Acme Corp"}


async def test_update_serializes_datetime(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify update() serializes datetime values to ISO strings."""

    class Event(BubbleModel, typename="event"):
        name: str
        start_time: datetime

    route = httpx2_mock.patch("https://example.com/event/abc123").respond(204)

    await Event.update(uid="abc123", start_time=datetime(2026, 1, 15, 14, 30, 0))

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"start_time": "2026-01-15T14:30:00"}


async def test_update_single_field(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify update() sends only the specified field."""

    class User(BubbleModel, typename="user"):
        name: str
        email: str

    route = httpx2_mock.patch("https://example.com/user/abc123").respond(204)

    await User.update(uid="abc123", name="New Name")

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"name": "New Name"}


async def test_update_translates_field_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify update() translates Python field names to Bubble aliases."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")
        status: str

    route = httpx2_mock.patch("https://example.com/order/xyz789").respond(204)

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


async def test_create_translates_field_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify create() translates Python field names to Bubble aliases."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")
        status: str

    route = httpx2_mock.post("https://example.com/order").respond(200, json={"status": "success", "id": "new123"})

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


async def test_create_or_update_translates_match_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify create_or_update() translates match field names to Bubble aliases."""
    from bubble_data_api_client.types import OnMultiple

    class Order(BubbleModel, typename="order"):
        external_id: str = Field(alias="External ID")
        company: str = Field(alias="Buying company")

    # mock find returning no results (will create)
    find_route = httpx2_mock.get("https://example.com/order").respond(
        200, json={"response": {"results": [], "count": 0, "remaining": 0}}
    )
    # mock create
    create_route = httpx2_mock.post("https://example.com/order").respond(
        200, json={"status": "success", "id": "new123"}
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


async def test_create_or_update_translates_data_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify create_or_update() translates data field names to Bubble aliases."""
    from bubble_data_api_client.types import OnMultiple

    class Order(BubbleModel, typename="order"):
        external_id: str = Field(alias="External ID")
        company: str = Field(alias="Buying company")

    # mock find returning one result (will update)
    httpx2_mock.get("https://example.com/order").respond(
        200, json={"response": {"results": [{"_id": "existing123"}], "count": 1, "remaining": 0}}
    )
    # mock update
    update_route = httpx2_mock.patch("https://example.com/order/existing123").respond(204)

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


async def test_find_iter_single_page(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_iter yields all items from a single page."""

    class User(BubbleModel, typename="user"):
        name: str

    httpx2_mock.get("https://example.com/user").respond(
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

    users = [user async for user in User.find_iter()]

    assert len(users) == 2
    assert users[0].uid == "1"
    assert users[0].name == "Alice"
    assert users[1].uid == "2"
    assert users[1].name == "Bob"


async def test_find_iter_multiple_pages(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_iter fetches all pages and yields items from each."""

    class User(BubbleModel, typename="user"):
        name: str

    route = httpx2_mock.get("https://example.com/user")
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


async def test_find_iter_empty_results(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_iter handles empty results."""

    class User(BubbleModel, typename="user"):
        name: str

    httpx2_mock.get("https://example.com/user").respond(
        200,
        json={"response": {"results": [], "count": 0, "remaining": 0}},
    )

    users = [user async for user in User.find_iter()]

    assert users == []


async def test_find_all_returns_list(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_all returns all items as a list."""

    class User(BubbleModel, typename="user"):
        name: str

    route = httpx2_mock.get("https://example.com/user")
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


async def test_find_iter_breaks_on_empty_page_with_nonzero_remaining(
    configured_client: None, httpx2_mock: respx.Router
) -> None:
    """Regression: find_iter must not infinite-loop past Bubble's ~50k cursor cap.

    Past the cursor cap, Bubble returns results=[] but continues to report a
    nonzero remaining. A loop driven only by `remaining > 0` would never
    advance (cursor += len(results) == 0) and would hang forever.
    """

    class User(BubbleModel, typename="user"):
        name: str

    route = httpx2_mock.get("https://example.com/user")
    route.side_effect = [
        # first page: real results, remaining still > 0 → loop continues
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [{"_id": "1", "name": "Alice"}],
                    "count": 1,
                    "remaining": 500,
                }
            },
        ),
        # second page: EMPTY results, remaining STILL > 0 (simulates past the
        # cursor cap). Loop must break on the empty page, not continue.
        httpx.Response(
            200,
            json={
                "response": {
                    "results": [],
                    "count": 0,
                    "remaining": 500,
                }
            },
        ),
    ]

    users = [u async for u in User.find_iter(page_size=1)]

    assert len(users) == 1
    assert users[0].name == "Alice"
    assert route.call_count == 2


async def test_find_page_returns_typed_models(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page returns a PageResult of typed model instances."""

    class User(BubbleModel, typename="user"):
        name: str

    httpx2_mock.get("https://example.com/user").respond(
        200,
        json={
            "response": {
                "cursor": 0,
                "results": [
                    {"_id": "1", "name": "Alice"},
                    {"_id": "2", "name": "Bob"},
                ],
                "count": 2,
                "remaining": 0,
            }
        },
    )

    page = await User.find_page()

    assert len(page.items) == 2
    assert all(isinstance(u, User) for u in page.items)
    assert page.items[0].name == "Alice"
    assert page.items[1].name == "Bob"
    assert page.cursor == 0
    assert page.remaining == 0
    assert page.total == 2
    assert page.has_more is False


async def test_find_page_middle_page_computes_total(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page computes total = cursor + len(items) + remaining."""

    class User(BubbleModel, typename="user"):
        name: str

    httpx2_mock.get("https://example.com/user").respond(
        200,
        json={
            "response": {
                "cursor": 100,
                "results": [{"_id": str(i), "name": f"u{i}"} for i in range(10)],
                "count": 10,
                "remaining": 40,
            }
        },
    )

    page = await User.find_page(cursor=100, limit=50)

    assert len(page.items) == 10
    assert page.cursor == 100
    assert page.remaining == 40
    assert page.total == 150
    assert page.has_more is True


async def test_find_page_empty_result(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page with zero matches returns an empty PageResult."""

    class User(BubbleModel, typename="user"):
        name: str

    httpx2_mock.get("https://example.com/user").respond(
        200,
        json={"response": {"cursor": 0, "results": [], "count": 0, "remaining": 0}},
    )

    page = await User.find_page()

    assert page.items == []
    assert page.total == 0
    assert page.has_more is False


async def test_find_page_translates_field_aliases(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page validates items through model aliases."""

    class Order(BubbleModel, typename="order"):
        company: str = Field(alias="Buying company")

    httpx2_mock.get("https://example.com/order").respond(
        200,
        json={
            "response": {
                "cursor": 0,
                "results": [{"_id": "abc", "Buying company": "Acme Corp"}],
                "count": 1,
                "remaining": 0,
            }
        },
    )

    page = await Order.find_page()

    assert len(page.items) == 1
    assert page.items[0].company == "Acme Corp"
    assert page.items[0].uid == "abc"


async def test_find_page_forwards_params(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page forwards constraints, sort, cursor, and limit."""

    class User(BubbleModel, typename="user"):
        name: str

    route = httpx2_mock.get("https://example.com/user").respond(
        200,
        json={"response": {"cursor": 20, "results": [], "count": 0, "remaining": 0}},
    )

    await User.find_page(
        constraints=[{"key": "name", "constraint_type": "equals", "value": "Alice"}],
        cursor=20,
        limit=15,
        sort_field="name",
        descending=True,
    )

    assert route.call_count == 1
    request_url = str(route.calls[0].request.url)
    assert "cursor=20" in request_url
    assert "limit=15" in request_url
    assert "sort_field=name" in request_url
    assert "descending=true" in request_url
    assert "constraints" in request_url
    assert "exclude_remaining" not in request_url


async def test_find_page_default_params(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify find_page defaults to cursor=0, limit=100."""

    class User(BubbleModel, typename="user"):
        name: str

    route = httpx2_mock.get("https://example.com/user").respond(
        200,
        json={"response": {"cursor": 0, "results": [], "count": 0, "remaining": 0}},
    )

    page = await User.find_page()

    assert route.call_count == 1
    request_url = str(route.calls[0].request.url)
    assert "cursor=0" in request_url
    assert "limit=100" in request_url
    assert page.cursor == 0


async def test_refresh_updates_instance_in_place(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify refresh() fetches data and updates the instance in place."""

    class User(BubbleModel, typename="user"):
        name: str
        email: str | None = None

    user = User(_id="abc123", name="Old Name", email=None)

    httpx2_mock.get("https://example.com/user/abc123").respond(
        200,
        json={"response": {"_id": "abc123", "name": "New Name", "email": "new@example.com"}},
    )

    result = await user.refresh()

    # verify instance was updated in place
    assert user.name == "New Name"
    assert user.email == "new@example.com"
    # verify returns self for chaining
    assert result is user


async def test_refresh_updates_server_computed_fields(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify refresh() populates server-computed fields like modified_date."""

    class User(BubbleModel, typename="user"):
        name: str

    user = User(_id="abc123", name="Test")
    assert user.modified_date is None

    httpx2_mock.get("https://example.com/user/abc123").respond(
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

    await user.refresh()

    assert user.created_date == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    assert user.modified_date == datetime(2024, 1, 16, 14, 20, 0, tzinfo=UTC)


async def test_refresh_raises_on_not_found(configured_client: None, httpx2_mock: respx.Router) -> None:
    """Verify refresh() raises BubbleAPIError when record no longer exists."""

    class User(BubbleModel, typename="user"):
        name: str

    user = User(_id="deleted123", name="Ghost")

    httpx2_mock.get("https://example.com/user/deleted123").respond(
        404,
        json={"body": {"status": "NOT_FOUND", "message": "Thing not found"}},
    )

    with pytest.raises(BubbleAPIError) as exc_info:
        await user.refresh()

    assert exc_info.value.status_code == 404
