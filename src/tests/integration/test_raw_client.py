import http
from collections.abc import AsyncGenerator

import httpx
import pytest

from bubble_data_api_client.client import raw_client


@pytest.fixture()
async def test_thing_id(bubble_raw_client: raw_client.RawClient, typename: str) -> AsyncGenerator[str]:
    """Create data in the bubble app and return the id of the created thing."""

    # create
    test_thing = {
        "text": "integration test",
    }
    response = await bubble_raw_client.create(typename, data=test_thing)
    bubble_id = response.json()["id"]

    # return
    yield bubble_id

    # delete
    await bubble_raw_client.delete(typename, uid=bubble_id)


async def test_retrieve_success(typename: str, test_thing_id: str, bubble_raw_client: raw_client.RawClient):
    """Test that we can retrieve a thing."""

    response = await bubble_raw_client.retrieve(typename=typename, uid=test_thing_id)
    assert isinstance(response, httpx.Response)

    response_body = response.json()
    assert "response" in response_body
    assert "_id" in response_body["response"]
    assert response_body["response"]["_id"] == test_thing_id
    assert "text" in response_body["response"]
    assert response_body["response"]["text"] == "integration test"


async def test_delete_success(typename: str, bubble_raw_client: raw_client.RawClient):
    """Test that we can delete a thing."""

    response_create = await bubble_raw_client.create(typename, data={"text": "integration test delete success"})
    assert isinstance(response_create, httpx.Response)

    response_body = response_create.json()
    assert "status" in response_body
    assert "id" in response_body
    unique_id = response_body["id"]

    response_delete = await bubble_raw_client.delete(typename=typename, uid=unique_id)
    # 204 No Content = success
    assert response_delete.status_code == http.HTTPStatus.NO_CONTENT
    # no response body
    assert response_delete.text == ""
