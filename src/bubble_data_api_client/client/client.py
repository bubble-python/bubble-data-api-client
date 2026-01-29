"""High-level client with data validation and transformation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from bubble_data_api_client.client.raw_client import RawClient


class BubbleResponseFields(BaseModel):
    """Fields nested under "response" in the API response."""

    results: list[dict]
    cursor: int
    count: int
    remaining: int


class BubbleDataApiResponseBody(BaseModel):
    """Contents of bubble data API response body."""

    response: BubbleResponseFields


class CreateThingSuccessResponse(BaseModel):
    """Response body returned when a thing is successfully created."""

    status: str
    id: str


class Bubble404ResponseBody(BaseModel):
    """Body content of a Bubble 404 not found response."""

    status: str
    message: str


class Bubble404Response(BaseModel):
    """Structured representation of a Bubble 404 response."""

    status_code: int = Field(404, alias="statusCode")
    body: Bubble404ResponseBody


class Client:
    """High-level Bubble API client with validation and error handling.

    Provides CRUD operations with data validation and transformation.
    Consider using BubbleModel for ORM-style access instead.
    """

    _data_api_root_url: str
    _api_key: str
    _raw_client: RawClient

    def __init__(
        self,
        data_api_root_url: str,
        api_key: str,
    ) -> None:
        """Initialize client with Bubble API credentials.

        Args:
            data_api_root_url: Base URL for the Bubble Data API.
            api_key: API key for authentication.
        """
        self._data_api_root_url = data_api_root_url
        self._api_key = api_key
