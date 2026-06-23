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

import asyncio
import http
import itertools
import typing
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field

from bubble_data_api_client.client.raw_client import (
    _DEFAULT_PAGE_SIZE,
    RawClient,
)
from bubble_data_api_client.constraints import (
    AdditionalSortField,
    Constraint,
    ConstraintType,
    constraint,
)
from bubble_data_api_client.exceptions import BubbleAPIError, UnknownFieldError
from bubble_data_api_client.pagination import _DEFAULT_KEYSET_WINDOW, _DEFAULT_SCAN_CONCURRENCY
from bubble_data_api_client.types import BUILTIN_FIELDS, BubbleField, OnMultiple, PageResult

# max UIDs per "in" constraint batch, matching the API's max page size.
_MAX_IN_CONSTRAINT_SIZE: int = 100


def _get_client() -> RawClient:
    return RawClient()


class BubbleModel(PydanticBaseModel):
    """Base class for Bubble data types with built-in fields and ORM operations."""

    _typename: typing.ClassVar[str]

    # aliases are spelled as plain strings (via .value) so that
    # cls.model_fields[name].alias is uniformly a `str` for callers, matching
    # the shape used by subclass-declared fields. BubbleField stays the single
    # source of truth for the literals.
    uid: str = Field(
        ...,
        alias=BubbleField.ID.value,
        description="Unique ID in format '{timestamp}x{random}' that identifies this record.",
    )
    created_by: str | None = Field(
        default=None,
        alias=BubbleField.CREATED_BY.value,
        description="Reference to the User who created this record. Empty when created without a logged-in user.",
    )
    created_date: datetime | None = Field(
        default=None,
        alias=BubbleField.CREATED_DATE.value,
        description="Creation date of this record. Never changes.",
    )
    modified_date: datetime | None = Field(
        default=None,
        alias=BubbleField.MODIFIED_DATE.value,
        description="Automatically updated when any changes are made to this record.",
    )
    slug: str | None = Field(
        default=None,
        alias=BubbleField.SLUG.value,
        description="User-friendly and SEO-optimized URL for this record.",
    )

    def __init_subclass__(cls, *, typename: str, **kwargs: typing.Any) -> None:
        """Register the Bubble type name for this model subclass."""
        super().__init_subclass__(**kwargs)
        cls._typename = typename

    @classmethod
    def bubble_field(cls, name: str) -> str:
        """Return the raw Bubble field name for a python attribute.

        Useful when building constraints, sort fields, or dispatch tables
        from Python attribute names without restating the Bubble alias at
        every call site.

        Args:
            name: A python attribute name on this model (e.g. ``"first_name"``).

        Returns:
            The Bubble field name. The declared ``Field(alias=...)`` if
            present, otherwise the python attribute name unchanged (matching
            Pydantic's ``by_alias=True`` serialization behavior).

        Raises:
            UnknownFieldError: ``name`` is not a field on this model.
        """
        field_info = cls.model_fields.get(name)
        if field_info is None:
            raise UnknownFieldError(name)
        return field_info.alias if field_info.alias is not None else name

    @classmethod
    def _serialize_for_api(cls, data: dict[str, typing.Any]) -> dict[str, typing.Any]:
        """Serialize field data for API requests with aliasing and JSON conversion."""
        for field_name in data:
            if field_name not in cls.model_fields:
                raise UnknownFieldError(field_name)
        partial = cls.model_construct(**data)
        return partial.model_dump(
            mode="json",
            include=set(data.keys()),
            by_alias=True,
        )

    @classmethod
    async def create(cls, **data: typing.Any) -> typing.Self:
        """Create a new thing in Bubble and return a model instance.

        Args:
            **data: Field values using Python field names (not Bubble aliases).

        Returns:
            A new model instance with the assigned Bubble UID.
        """
        aliased_data = cls._serialize_for_api(data)
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
        if len(uids) <= _MAX_IN_CONSTRAINT_SIZE:
            items: list[typing.Self] = await cls.find_all(
                constraints=[constraint(BubbleField.ID, ConstraintType.IN, uids)],
            )
            return {item.uid: item for item in items}
        chunks = itertools.batched(uids, _MAX_IN_CONSTRAINT_SIZE, strict=False)
        chunk_results: list[dict[str, typing.Self]] = await asyncio.gather(
            *[cls.get_many(list(chunk)) for chunk in chunks],
        )
        return {uid: item for result in chunk_results for uid, item in result.items()}

    async def save(self) -> None:
        """Persist all field changes to Bubble.

        Saves all model fields except uid and the read-only built-in fields
        (created_by, created_date, modified_date, slug).
        """
        async with _get_client() as client:
            # exclude uid and the read-only built-in fields
            data = self.model_dump(
                mode="json",
                exclude=BUILTIN_FIELDS,
                by_alias=True,
            )
            await client.update(self._typename, self.uid, data)

    @classmethod
    async def update(cls, uid: str, **data: typing.Any) -> None:
        """Update specific fields on a thing by its unique ID."""
        aliased_data = cls._serialize_for_api(data)
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
    async def find_page(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        cursor: int = 0,
        limit: int = _DEFAULT_PAGE_SIZE,
        sort_field: str | None = None,
        descending: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> PageResult[typing.Self]:
        """Return one page of matching records with envelope metadata.

        Unlike find(), this preserves Bubble's response envelope so callers
        can display total counts and drive pagination UIs without issuing a
        separate count() call.

        See RawClient.find_page for important caveats about Bubble's
        pagination limits: the 100-item silent cap on limit, the ~50,000
        cursor cap on shared infrastructure, and the recommended keyset
        pagination workaround for collections larger than the cursor cap.

        Args:
            constraints: Filter conditions (use constraint() helper to build).
            cursor: Pagination offset (0-indexed).
            limit: Maximum results to return on this page.
            sort_field: Field name to sort by.
            descending: Sort in descending order if True.
            additional_sort_fields: Secondary sort fields after the primary.

        Returns:
            PageResult with typed model instances plus envelope metadata.
        """
        async with _get_client() as client:
            page = await client.find_page(
                cls._typename,
                constraints=constraints,
                cursor=cursor,
                limit=limit,
                sort_field=sort_field,
                descending=descending,
                additional_sort_fields=additional_sort_fields,
            )
            return PageResult(
                items=[cls.model_validate(item) for item in page.items],
                cursor=page.cursor,
                remaining=page.remaining,
            )

    @classmethod
    async def find_iter(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
        sort_field: str | None = None,
        descending: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> AsyncIterator[typing.Self]:
        """Iterate through all matching records with constant memory usage.

        Offset pagination is capped at ~50,000 on Bubble's shared
        infrastructure. For collections larger than that cap, prefer
        keyset pagination (a Created Date constraint) instead of this
        method, which will stop short at the cap. The empty-page guard
        below prevents an infinite loop when the cursor reaches the cap
        (Bubble continues to report a non-zero remaining past the cap).
        """
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
                # stop on empty page first: past the cursor cap Bubble can
                # return zero results while still reporting remaining > 0,
                # which would otherwise cause an infinite loop.
                if not body["results"]:
                    break
                if body["remaining"] == 0:
                    break
                cursor += len(body["results"])

    @classmethod
    async def find_all(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        page_size: int = _DEFAULT_PAGE_SIZE,
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
    async def scan(
        cls,
        *,
        constraints: list[Constraint] | None = None,
        keyset_field: str = BubbleField.CREATED_DATE,
        page_size: int = _DEFAULT_PAGE_SIZE,
        window: int = _DEFAULT_KEYSET_WINDOW,
        concurrency: int = _DEFAULT_SCAN_CONCURRENCY,
    ) -> AsyncIterator[typing.Self]:
        """Stream every matching record, ordered by keyset_field ascending.

        Use scan() instead of find_iter()/find_all() for collections larger
        than Bubble's ~50,000 cursor cap. It uses keyset pagination, so it
        streams records of any size with constant memory, where the offset
        pagination behind find_iter() silently stops at the cap.

        The trade-off is fixed ordering: records arrive in ascending
        keyset_field order (Created Date by default) and an arbitrary sort
        cannot be combined with cap-free iteration. See
        bubble_data_api_client.pagination for the algorithm and its limits.

        Args:
            constraints: Filter conditions (use constraint() helper to build).
            keyset_field: Monotonic date field to page by. Defaults to Created Date.
            page_size: Records requested per page (Bubble caps this at 100).
            window: Cursor offset at which to seek forward. Below the ~50k cap.
            concurrency: Maximum pages fetched in parallel. 1 (the default)
                fetches strictly sequentially. Higher values multiply both
                throughput and request rate against Bubble; ordering and
                no-duplicate guarantees are unchanged.

        Yields:
            Model instances in ascending keyset_field order.
        """
        async with _get_client() as client:
            async for row in client.scan(
                cls._typename,
                constraints=constraints,
                keyset_field=keyset_field,
                page_size=page_size,
                window=window,
                concurrency=concurrency,
            ):
                yield cls.model_validate(row)

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
        aliased_match = cls._serialize_for_api(match)
        aliased_create_data = cls._serialize_for_api(create_data) if create_data else None
        aliased_update_data = cls._serialize_for_api(update_data) if update_data else None
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
