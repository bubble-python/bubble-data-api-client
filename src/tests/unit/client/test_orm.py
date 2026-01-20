import json

import httpx
import pytest
import respx
from pydantic import Field

from bubble_data_api_client.client.orm import BubbleBaseModel
from bubble_data_api_client.exceptions import UnknownFieldError


def test_model_instantiation():
    """Tests that the Pydantic model can be instantiated."""

    class User(BubbleBaseModel, typename="user"):
        name: str

    # instantiate the model, no client is needed
    user = User(name="testuser", _id="12345")

    assert user.uid == "12345"
    assert user.name == "testuser"


@respx.mock
async def test_save_uses_field_aliases(configured_client: None) -> None:
    """Verify save() sends Bubble aliases, not Python field names."""

    class Order(BubbleBaseModel, typename="order"):
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

    class User(BubbleBaseModel, typename="user"):
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

    class Order(BubbleBaseModel, typename="order"):
        company: str = Field(alias="Buying company")
        status: str

    route = respx.patch("https://example.com/order/xyz789").mock(return_value=httpx.Response(204))

    await Order.update(uid="xyz789", company="Acme Corp", status="active")

    assert route.call_count == 1
    request_body = json.loads(route.calls[0].request.content)
    assert request_body == {"Buying company": "Acme Corp", "status": "active"}


async def test_update_raises_for_unknown_field() -> None:
    """Verify update() raises UnknownFieldError for fields not in the model."""

    class User(BubbleBaseModel, typename="user"):
        name: str

    with pytest.raises(UnknownFieldError, match="unknown field: nonexistent"):
        await User.update(uid="abc123", nonexistent="value")
