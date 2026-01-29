"""ORM-style base class for Bubble data types.

Define models by subclassing BubbleModel with a typename parameter:

    class User(BubbleModel, typename="user"):
        name: str
        email: str | None = None

Then use async CRUD operations:
    user = await User.create(name="Alice")
    user = await User.get("1234x5678")
    users = await User.find(constraints=[constraint("name", ConstraintType.EQUALS, "Alice")])
    await user.save()
    await user.delete()
"""

from __future__ import annotations

import http
import typing
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field

from bubble_data_api_client.client.raw_client import AdditionalSortField, RawClient
from bubble_data_api_client.constraints import Constraint, ConstraintType, constraint
from bubble_data_api_client.exceptions import BubbleAPIError, UnknownFieldError
from bubble_data_api_client.types import BubbleField, OnMultiple


def _get_client() -> RawClient:
    return RawClient()


class BubbleModel(PydanticBaseModel):
    """Base class for Bubble data types with built-in fields and ORM operations."""

    _typename: typing.ClassVar[str]

    uid: str = Field(
        ...,
        alias=BubbleField.ID,
        description="Unique ID in format '{timestamp}x{random}' that identifies this record.",
    )
    created_date: datetime | None = Field(
        default=None,
        alias=BubbleField.CREATED_DATE,
        description="Creation date of this record. Never changes.",
    )
    modified_date: datetime | None = Field(
        default=None,
        alias=BubbleField.MODIFIED_DATE,
        description="Automatically updated when any changes are made to this record.",
    )
    slug: str | None = Field(
        default=None,
        alias=BubbleField.SLUG,
        description="User-friendly and SEO-optimized URL for this record.",
    )

    def __init_subclass__(cls, *, typename: str, **kwargs: typing.Any) -> None:
        """Register the Bubble type name for this model subclass."""
        super().__init_subclass__(**kwargs)
        cls._typename = typename

    @classmethod
    def _resolve_aliases(cls, data: dict[str, typing.Any]) -> dict[str, typing.Any]:
        """Translate ORM field names to their aliases for API requests."""
        resolved: dict[str, typing.Any] = {}
        for field_name, value in data.items():
            field_info = cls.model_fields.get(field_name)
            if field_info is None:
                raise UnknownFieldError(field_name)
            if field_info.alias:
                resolved[field_info.alias] = value
            else:
                resolved[field_name] = value
        return resolved

    @classmethod
    async def create(cls, **data: typing.Any) -> typing.Self:
        """Create a new thing in Bubble and return a model instance.

        Args:
            **data: Field values using Python field names (not Bubble aliases).

        Returns:
            A new model instance with the assigned Bubble UID.
        """
        aliased_data = cls._resolve_aliases(data)
        async with _get_client() as client:
            response = await client.create(cls._typename, aliased_data)
            uid = response.json()["id"]
            return cls.model_validate({**aliased_data, BubbleField.ID: uid})

    @classmethod
    async def get(cls, uid: str) -> typing.Self | None:
        """Retrieve a single thing by its unique ID."""
        async with _get_client() as client:
            try:
                response = await client.retrieve(cls._typename, uid)
                return cls.model_validate(response.json()["response"])
            except BubbleAPIError as e:
                if e.status_code == http.HTTPStatus.NOT_FOUND:
                    return None
                raise

    @classmethod
    async def get_many(cls, uids: list[str]) -> dict[str, typing.Self]:
        """Retrieve multiple things by their unique IDs, keyed by uid."""
        if not uids:
            return {}
        items: list[typing.Self] = await cls.find(
            constraints=[constraint(BubbleField.ID, ConstraintType.IN, uids)],
        )
        return {item.uid: item for item in items}

    async def save(self) -> None:
        """Persist all field changes to Bubble.

        Saves all model fields except uid and server-managed fields
        (created_date, modified_date, slug).
        """
        async with _get_client() as client:
            # exclude uid and server-managed fields
            data = self.model_dump(
                exclude={"uid", "created_date", "modified_date", "slug"},
                by_alias=True,
            )
            await client.update(self._typename, self.uid, data)

    @classmethod
    async def update(cls, uid: str, **data: typing.Any) -> None:
        """Update specific fields on a thing by its unique ID."""
        aliased_data = cls._resolve_aliases(data)
        async with _get_client() as client:
            await client.update(cls._typename, uid, aliased_data)

    async def delete(self) -> None:
        """Delete this thing from Bubble."""
        async with _get_client() as client:
            await client.delete(self._typename, self.uid)

    async def refresh(self) -> typing.Self:
        """Fetch latest data from Bubble and update this instance in place.

        Useful after create_or_update() to get server-computed fields like
        Modified Date, or fields set by Bubble workflows.

        Returns:
            Self, for method chaining.

        Raises:
            BubbleAPIError: If the record no longer exists (404) or other API error.
        """
        async with _get_client() as client:
            response = await client.retrieve(self._typename, self.uid)
            cls = type(self)
            fresh = cls.model_validate(response.json()["response"])
            for field_name in cls.model_fields:
                setattr(self, field_name, getattr(fresh, field_name))
            return self

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
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> list[typing.Self]:
        """Search for things matching the given constraints.

        Args:
            constraints: Filter conditions (use constraint() helper to build).
            cursor: Pagination offset (0-indexed).
            limit: Maximum results to return (default 100, max varies by plan).
            sort_field: Field name to sort by.
            descending: Sort in descending order if True.
            exclude_remaining: Skip counting remaining results for performance.
            additional_sort_fields: Secondary sort fields after the primary.

        Returns:
            List of matching model instances.
        """
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
            return [cls.model_validate(item) for item in response.json()["response"]["results"]]

    @classmethod
    async def find_iter(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        page_size: int = 100,
        sort_field: str | None = None,
        descending: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> AsyncIterator[typing.Self]:
        """Iterate through all matching records with constant memory usage."""
        cursor: int = 0
        async with _get_client() as client:
            while True:
                response = await client.find(
                    cls._typename,
                    constraints=constraints,
                    cursor=cursor,
                    limit=page_size,
                    sort_field=sort_field,
                    descending=descending,
                    additional_sort_fields=additional_sort_fields,
                )
                body = response.json()["response"]
                for item in body["results"]:
                    yield cls.model_validate(item)
                if body["remaining"] == 0:
                    break
                cursor += len(body["results"])

    @classmethod
    async def find_all(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        page_size: int = 100,
        sort_field: str | None = None,
        descending: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> list[typing.Self]:
        """Return all matching records as a list."""
        return [
            item
            async for item in cls.find_iter(
                constraints=constraints,
                page_size=page_size,
                sort_field=sort_field,
                descending=descending,
                additional_sort_fields=additional_sort_fields,
            )
        ]

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

    @classmethod
    async def create_or_update(
        cls,
        *,
        match: dict[str, typing.Any],
        create_data: dict[str, typing.Any] | None = None,
        update_data: dict[str, typing.Any] | None = None,
        on_multiple: OnMultiple,
    ) -> tuple[typing.Self, bool]:
        """Create a thing if it doesn't exist, or update if it does."""
        aliased_match = cls._resolve_aliases(match)
        aliased_create_data = cls._resolve_aliases(create_data) if create_data else None
        aliased_update_data = cls._resolve_aliases(update_data) if update_data else None
        async with _get_client() as client:
            result = await client.create_or_update(
                typename=cls._typename,
                match=aliased_match,
                create_data=aliased_create_data,
                update_data=aliased_update_data,
                on_multiple=on_multiple,
            )
            # construct instance from aliased data
            # server-side fields like Modified Date won't be populated
            instance_data = (aliased_create_data or {}) if result["created"] else (aliased_update_data or {})
            model_data = {**aliased_match, **instance_data, BubbleField.ID: result["uids"][0]}
            return cls.model_validate(model_data), result["created"]
