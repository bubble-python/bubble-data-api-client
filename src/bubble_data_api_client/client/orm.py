import http
import typing

import httpx
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field

from bubble_data_api_client.client.raw_client import RawClient
from bubble_data_api_client.constraints import Constraint, ConstraintTypes, constraint


def _get_client() -> RawClient:
    return RawClient()


class BubbleBaseModel(PydanticBaseModel):
    _typename: typing.ClassVar[str]

    uid: str = Field(..., alias="_id")

    def __init_subclass__(cls, *, typename: str, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._typename = typename

    @classmethod
    async def create(cls, **data: typing.Any) -> typing.Self:
        async with _get_client() as client:
            response = await client.create(cls._typename, data)
            response.raise_for_status()
            uid = response.json()["id"]
            return cls(**data, _id=uid)

    @classmethod
    async def get(cls, uid: str) -> typing.Self | None:
        """Retrieve a single thing by its unique ID."""
        async with _get_client() as client:
            try:
                response = await client.retrieve(cls._typename, uid)
                response.raise_for_status()
                return cls(**response.json()["response"])
            except httpx.HTTPStatusError as e:
                if e.response.status_code == http.HTTPStatus.NOT_FOUND:
                    return None
                raise

    @classmethod
    async def get_many(cls, uids: list[str]) -> dict[str, typing.Self]:
        """Retrieve multiple things by their unique IDs, keyed by uid."""
        if not uids:
            return {}
        items: list[typing.Self] = await cls.find(
            constraints=[constraint("_id", ConstraintTypes.IN, uids)],
        )
        return {item.uid: item for item in items}

    async def save(self) -> None:
        async with _get_client() as client:
            data = self.model_dump(exclude={"uid"})
            response = await client.update(self._typename, self.uid, data)
            response.raise_for_status()

    async def delete(self) -> None:
        async with _get_client() as client:
            response = await client.delete(self._typename, self.uid)
            response.raise_for_status()

    @classmethod
    async def find(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        cursor: int | None = None,
        limit: int | None = None,
        sort_field: str | None = None,
        descending: bool | None = None,
        exclude_remaining: bool | None = None,
        additional_sort_fields: list | None = None,
    ) -> list[typing.Self]:
        async with _get_client() as client:
            response = await client.find(
                cls._typename,
                constraints=constraints,
                cursor=cursor,
                limit=limit,
                sort_field=sort_field,
                descending=descending,
                exclude_remaining=exclude_remaining,
                additional_sort_fields=additional_sort_fields,
            )
            response.raise_for_status()
            return [cls(**item) for item in response.json()["response"]["results"]]

    @classmethod
    async def count(cls, *, constraints: list[Constraint] | None = None) -> int:
        """Return total count of objects matching constraints."""
        async with _get_client() as client:
            return await client.count(cls._typename, constraints=constraints)

    @classmethod
    async def exists(
        cls,
        uid: str | None = None,
        *,
        constraints: list[Constraint] | None = None,
    ) -> bool:
        """Check if record(s) exist by ID or constraints."""
        async with _get_client() as client:
            return await client.exists(cls._typename, uid=uid, constraints=constraints)
