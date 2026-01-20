import json

import httpx
import respx
from pydantic import Field

from bubble_data_api_client.client.orm import BubbleBaseModel


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
