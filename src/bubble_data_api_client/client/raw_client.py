import asyncio
import http
import json
import types
import typing

import httpx

from bubble_data_api_client.constraints import Constraint, ConstraintType, constraint
from bubble_data_api_client.exceptions import InvalidOnMultipleError, MultipleMatchesError, PartialFailureError
from bubble_data_api_client.transport import Transport
from bubble_data_api_client.types import BubbleField, CreateOrUpdateResult, OnMultiple


# https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#sorting
# in addition to 'sort_field' and 'descending', it is possible to have
# multiple additional sort fields
class AdditionalSortField(typing.TypedDict):
    sort_field: str
    descending: bool


class RawClient:
    """
    Raw Client layer focuses on bubble.io API endpoints.

    https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests
    https://www.postman.com/bubbleapi/bubble/request/jigyk5v/
    """

    _transport: Transport

    def __init__(self) -> None:
        pass

    async def __aenter__(self) -> typing.Self:
        self._transport = Transport()
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self._transport.__aexit__(exc_type, exc_val, exc_tb)

    async def retrieve(self, typename: str, uid: str) -> httpx.Response:
        return await self._transport.get(f"/{typename}/{uid}")

    async def create(self, typename: str, data: typing.Any) -> httpx.Response:
        return await self._transport.post(url=f"/{typename}", json=data)

    async def bulk_create(self, typename: str, data: list[typing.Any]) -> httpx.Response:
        return await self._transport.post_text(
            url=f"/{typename}/bulk",
            content="\n".join(json.dumps(item) for item in data),
        )

    async def delete(self, typename: str, uid: str) -> httpx.Response:
        return await self._transport.delete(f"/{typename}/{uid}")

    async def update(self, typename: str, uid: str, data: typing.Any) -> httpx.Response:
        return await self._transport.patch(f"/{typename}/{uid}", json=data)

    async def replace(self, typename: str, uid: str, data: typing.Any) -> httpx.Response:
        return await self._transport.put(f"/{typename}/{uid}", json=data)

    # https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#get-a-list-of-things
    async def find(
        self,
        typename: str,
        *,
        constraints: list[Constraint] | None = None,
        cursor: int | None = None,
        limit: int | None = None,
        sort_field: str | None = None,
        descending: bool | None = None,
        exclude_remaining: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> httpx.Response:
        params: dict[str, str] = {}

        if constraints is not None:
            params["constraints"] = json.dumps(constraints)
        if cursor is not None:
            params["cursor"] = str(cursor)
        if limit is not None:
            params["limit"] = str(limit)
        if sort_field is not None:
            params["sort_field"] = str(sort_field)
        if descending is not None:
            params["descending"] = "true" if descending else "false"
        if exclude_remaining is not None:
            params["exclude_remaining"] = "true" if exclude_remaining else "false"
        if additional_sort_fields is not None:
            params["additional_sort_fields"] = json.dumps(additional_sort_fields)

        return await self._transport.get(f"/{typename}", params=params)

    async def count(
        self,
        typename: str,
        *,
        constraints: list[Constraint] | None = None,
    ) -> int:
        """Return total count of objects matching constraints."""
        response = await self.find(typename, constraints=constraints, limit=1)
        body = response.json()["response"]
        return body["count"] + body["remaining"]

    async def exists(
        self,
        typename: str,
        uid: str | None = None,
        *,
        constraints: list[Constraint] | None = None,
    ) -> bool:
        """Check if record(s) exist by ID or constraints."""
        if uid is not None and constraints is not None:
            msg = "Cannot specify both uid and constraints"
            raise ValueError(msg)

        if uid is not None:
            # ID lookup: retrieve + 404 is optimal (no JSON parsing needed)
            try:
                await self.retrieve(typename, uid)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == http.HTTPStatus.NOT_FOUND:
                    return False
                raise
            else:
                return True

        # constraint-based: find with exclude_remaining for DB short-circuit
        response = await self.find(
            typename,
            constraints=constraints,
            limit=1,
            exclude_remaining=True,
        )
        return response.json()["response"]["count"] >= 1

    async def create_or_update(
        self,
        typename: str,
        *,
        match: dict[str, typing.Any],
        data: dict[str, typing.Any],
        on_multiple: OnMultiple,
    ) -> CreateOrUpdateResult:
        """Create a thing if it doesn't exist, or update if it does."""
        # Note: Bubble Data API does not support atomic upsert. This operation has a race
        # condition between the find and create/update steps. If two processes call this
        # with the same match fields simultaneously, both may see "not found" and create
        # duplicates. Bubble does not support unique constraints either, so this cannot
        # be prevented at the API level.
        #
        # on_multiple controls behavior when multiple things match:
        # - ERROR: raise MultipleMatchesError
        # - UPDATE_FIRST: update the first match (arbitrary order)
        # - UPDATE_ALL: update all matches (N API calls, no bulk update in Bubble)
        # - DEDUPE_OLDEST: delete all but oldest, update oldest (N API calls)
        # - DEDUPE_NEWEST: delete all but newest, update newest (N API calls)

        if on_multiple not in OnMultiple:
            raise InvalidOnMultipleError(on_multiple)

        # empty match would produce zero constraints, causing find() to return ALL records
        if not match:
            msg = "match cannot be empty"
            raise ValueError(msg)

        # empty data means nothing to update/set beyond match fields
        if not data:
            msg = "data cannot be empty"
            raise ValueError(msg)

        # build equals constraints from match fields to find existing thing
        constraints: list[Constraint] = [
            constraint(key=key, constraint_type=ConstraintType.EQUALS, value=value) for key, value in match.items()
        ]

        # for dedupe strategies, sort by created date to determine oldest/newest
        sort_field: str | None = None
        descending: bool | None = None
        if on_multiple in (OnMultiple.DEDUPE_OLDEST, OnMultiple.DEDUPE_NEWEST):
            sort_field = BubbleField.CREATED_DATE
            descending = on_multiple == OnMultiple.DEDUPE_NEWEST

        response = await self.find(
            typename=typename,
            constraints=constraints,
            sort_field=sort_field,
            descending=descending,
        )
        results: list[dict[str, typing.Any]] = response.json()["response"]["results"]

        # no matches: create new thing
        if not results:
            create_data = {**match, **data}
            response = await self.create(typename=typename, data=create_data)
            response.raise_for_status()
            uid: str = response.json()["id"]
            return {"uids": [uid], "created": True}

        # single match: update it
        if len(results) == 1:
            uid = results[0][BubbleField.ID]
            response = await self.update(typename=typename, uid=uid, data=data)
            response.raise_for_status()
            return {"uids": [uid], "created": False}

        # multiple matches: handle according to strategy
        match on_multiple:
            case OnMultiple.ERROR:
                raise MultipleMatchesError(typename=typename, count=len(results), match=match)

            case OnMultiple.UPDATE_FIRST:
                uid = results[0][BubbleField.ID]
                response = await self.update(typename=typename, uid=uid, data=data)
                response.raise_for_status()
                return {"uids": [uid], "created": False}

            case OnMultiple.UPDATE_ALL:
                # bubble does not support bulk PATCH, so we update concurrently
                uids = [result[BubbleField.ID] for result in results]
                results_or_errors = await asyncio.gather(
                    *[self.update(typename=typename, uid=uid, data=data) for uid in uids],
                    return_exceptions=True,
                )

                # check for failures, letting all operations complete before raising
                succeeded: list[str] = []
                failed: list[tuple[str, BaseException]] = []
                for uid, item in zip(uids, results_or_errors, strict=True):
                    if isinstance(item, BaseException):
                        failed.append((uid, item))
                    else:
                        item.raise_for_status()
                        succeeded.append(uid)

                if failed:
                    raise PartialFailureError(
                        operation="update",
                        succeeded=succeeded,
                        failed=failed,
                    )
                return {"uids": uids, "created": False}

            case OnMultiple.DEDUPE_OLDEST | OnMultiple.DEDUPE_NEWEST:
                # first result is the one to keep (already sorted)
                keep_uid = results[0][BubbleField.ID]
                delete_uids = [r[BubbleField.ID] for r in results[1:]]

                # update first so data is preserved even if deletes fail
                response = await self.update(typename=typename, uid=keep_uid, data=data)
                response.raise_for_status()

                # delete duplicates concurrently, letting all complete before checking errors
                delete_results = await asyncio.gather(
                    *[self.delete(typename=typename, uid=uid) for uid in delete_uids],
                    return_exceptions=True,
                )

                succeeded: list[str] = []
                failed: list[tuple[str, BaseException]] = []
                for uid, item in zip(delete_uids, delete_results, strict=True):
                    if isinstance(item, BaseException):
                        failed.append((uid, item))
                    else:
                        item.raise_for_status()
                        succeeded.append(uid)

                if failed:
                    raise PartialFailureError(
                        operation="delete",
                        succeeded=succeeded,
                        failed=failed,
                    )
                return {"uids": [keep_uid], "created": False}
