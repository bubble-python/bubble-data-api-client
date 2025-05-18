from pydantic import BaseModel, Field

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
    status: str
    id: str


class Bubble404ResponseBody(BaseModel):
    status: str
    message: str


class Bubble404Response(BaseModel):
    status_code: int = Field(404, alias="statusCode")
    body: Bubble404ResponseBody


class Client:
    """
    Client layer focuses on providing a convenient interface.
    - CRUD operations
    - data validation
    - data transformation
    - error handling
    """

    _data_api_root_url: str
    _api_key: str
    _raw_client: RawClient

    def __init__(
        self,
        data_api_root_url: str,
        api_key: str,
    ):
        self._data_api_root_url = data_api_root_url
        self._api_key = api_key
