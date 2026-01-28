import http
import json
import warnings
from collections.abc import AsyncGenerator

import httpx
import pytest

from bubble_data_api_client.client import raw_client
from bubble_data_api_client.types import BubbleField


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
    assert BubbleField.ID in response_body["response"]
    assert response_body["response"][BubbleField.ID] == test_thing_id
    assert "text" in response_body["response"]
    assert response_body["response"]["text"] == "integration test"


async def test_bulk_create_success(typename: str, bubble_raw_client: raw_client.RawClient):
    """Test that bulk_create creates multiple things and returns their IDs."""
    created_ids: list[str] = []

    try:
        response = await bubble_raw_client.bulk_create(
            typename=typename,
            data=[{"text": "bulk test 1"}, {"text": "bulk test 2"}],
        )

        # bubble returns text/plain with newline-delimited JSON
        assert response.status_code == http.HTTPStatus.OK
        assert response.headers["content-type"] == "text/plain"
        lines = response.text.strip().split("\n")
        assert len(lines) == 2

        # parse each line and extract IDs for cleanup
        for line in lines:
            result = json.loads(line)
            assert result["status"] == "success"
            assert "id" in result
            created_ids.append(result["id"])

        # verify both items exist with correct data
        expected_texts = ["bulk test 1", "bulk test 2"]
        for uid, expected_text in zip(created_ids, expected_texts, strict=True):
            retrieve_response = await bubble_raw_client.retrieve(typename=typename, uid=uid)
            assert retrieve_response.status_code == http.HTTPStatus.OK
            assert retrieve_response.json()["response"]["text"] == expected_text

    finally:
        # cleanup all created items
        for uid in created_ids:
            try:
                await bubble_raw_client.delete(typename=typename, uid=uid)
            except Exception as e:
                warnings.warn(f"cleanup failed for {uid}: {e}", stacklevel=2)


async def test_bulk_create_parsed_success(typename: str, bubble_raw_client: raw_client.RawClient):
    """Test that bulk_create_parsed returns typed results."""
    created_ids: list[str] = []

    try:
        results = await bubble_raw_client.bulk_create_parsed(
            typename=typename,
            data=[{"text": "parsed test 1"}, {"text": "parsed test 2"}],
        )

        assert len(results) == 2
        for result in results:
            assert result["status"] == "success"
            assert result["id"] is not None
            assert result["message"] is None
            created_ids.append(result["id"])

        # verify items exist with correct data
        expected_texts = ["parsed test 1", "parsed test 2"]
        for uid, expected_text in zip(created_ids, expected_texts, strict=True):
            retrieve_response = await bubble_raw_client.retrieve(typename=typename, uid=uid)
            assert retrieve_response.json()["response"]["text"] == expected_text

    finally:
        for uid in created_ids:
            try:
                await bubble_raw_client.delete(typename=typename, uid=uid)
            except Exception as e:
                warnings.warn(f"cleanup failed for {uid}: {e}", stacklevel=2)


async def test_update_success(typename: str, test_thing_id: str, bubble_raw_client: raw_client.RawClient):
    """Test that we can update a thing."""
    response = await bubble_raw_client.update(typename=typename, uid=test_thing_id, data={"text": "updated text"})
    # 204 No Content = success
    assert response.status_code == http.HTTPStatus.NO_CONTENT

    # verify the update
    response = await bubble_raw_client.retrieve(typename=typename, uid=test_thing_id)
    assert response.json()["response"]["text"] == "updated text"


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
