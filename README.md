
# bubble-data-api-client

[![Downloads](https://static.pepy.tech/badge/bubble-data-api-client/month)](https://pepy.tech/project/bubble-data-api-client)
[![Python Version](https://img.shields.io/pypi/pyversions/bubble-data-api-client)](https://pypi.org/project/bubble-data-api-client/)
[![License](https://img.shields.io/pypi/l/bubble-data-api-client)](https://pypi.org/project/bubble-data-api-client/)
[![PyPI](https://img.shields.io/pypi/v/bubble-data-api-client)](https://pypi.org/project/bubble-data-api-client/)

A fast, async Python client for the [Bubble Data API](https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api) with a Pydantic-based ORM and connection pooling.

## Why use this?

**If you're integrating Python with a Bubble app**, this library handles the boilerplate so you can focus on your logic.

**Common use cases:**
- Syncing data between Bubble and external systems
- Data migrations and bulk imports
- Backend scripts and automation
- Reporting that pulls from Bubble's database

### Clean, simple interface

```python
# create
user = await User.create(name="Ada", email="ada@example.com")

# retrieve
user = await User.get(uid)

# query (paginated)
users = await User.find(constraints=[
    constraint("status", ConstraintType.EQUALS, "active")
])

# query all matching records
all_users = await User.find_all()

# iterate with constant memory
async for user in User.find_iter():
    process(user)

# update
user.name = "Ada Lovelace"
await user.save()

# delete
await user.delete()

# check existence
if await User.exists(uid):
    print("User exists")

# count
active_count = await User.count(constraints=[
    constraint("status", ConstraintType.EQUALS, "active")
])
```

### IDE support and type checking

Models provide autocomplete and catch errors before runtime:

```python
class User(BubbleModel, typename="user"):
    name: str
    email: str
    age: int

user = await User.get(uid)
user.name    # IDE autocomplete works
user.nme     # Typo caught by pyright/mypy
```

Works with pyright, mypy, and IDE type checkers.

### Validation catches bad data early

Pydantic validates data when models are created:

```python
# Type mismatch caught immediately
user = User(_id="123x456", name="Ada", email="ada@example.com", age="twenty-five")
# ValidationError: Input should be a valid integer

# Invalid Bubble UID caught at the model level
class Order(BubbleModel, typename="order"):
    customer: BubbleUID

order = Order(_id="123x456", customer="not-a-valid-uid")
# ValidationError: invalid Bubble UID format: not-a-valid-uid
```

### Bubble-specific handling

The library handles Bubble's API quirks automatically:
- **Field mapping**: Bubble's `_id` field maps to `uid` on your models
- **Response parsing**: Extracts data from Bubble's nested `{"response": {"results": [...]}}` structure
- **Constraint format**: Builds the JSON constraint format Bubble expects

### Duplicate handling

Bubble doesn't enforce unique constraints, so duplicates can occur. The `create_or_update` method provides strategies to handle this:

```python
# If duplicates exist, keep the oldest and delete the rest
user, created = await User.create_or_update(
    match={"external_id": "ext-123"},
    data={"name": "Canonical Name"},
    on_multiple=OnMultiple.DEDUPE_OLDEST,
)
```

### Connection reuse

HTTP connections are pooled per event loop, avoiding reconnection overhead when making multiple requests

## Features

- **Async-first:** built on `httpx` with HTTP/2
- **Pydantic ORM:** define models once, get validation and autocomplete
- **Connection pooling:** automatic per-event-loop client reuse
- **Rich query constraints:** pythonic filtering using Bubble's constraint system
- **Efficient iteration:** `find_iter()` streams records with constant memory
- **Upsert with duplicate handling:** `create_or_update` with configurable strategies
- **Configurable retries:** plug in your own retry policy via `tenacity`
- **UID validation:** catch invalid Bubble IDs at the model level

## Installation

```bash
pip install bubble-data-api-client
```

Requires Python 3.13+.

## Quick Start

### Configuration

```python
from bubble_data_api_client import configure

configure(
    data_api_root_url="https://your-app.bubbleapps.io/api/1.1/obj",
    api_key="your-api-key",
)
```

Or use a dynamic provider for secrets management:

```python
import os
from bubble_data_api_client import set_config_provider, BubbleConfig

def get_config() -> BubbleConfig:
    return BubbleConfig(
        data_api_root_url=os.environ["BUBBLE_API_URL"],
        api_key=os.environ["BUBBLE_API_KEY"],
    )

set_config_provider(get_config)
```

### Using the ORM

Define typed models with validation:

```python
from bubble_data_api_client import BubbleModel, BubbleUID, OptionalBubbleUID

class User(BubbleModel, typename="user"):
    name: str
    email: str
    company: OptionalBubbleUID = None  # linked Bubble record

class Company(BubbleModel, typename="company"):
    name: str
    industry: str
```

Then use them:

```python
# create
user = await User.create(name="Ada Lovelace", email="ada@example.com")

# retrieve
user = await User.get("1234567890x1234567890")

# query with constraints (single page)
from bubble_data_api_client import constraint, ConstraintType

active_users = await User.find(constraints=[
    constraint("status", ConstraintType.EQUALS, "active"),
    constraint("age", ConstraintType.GREATER_THAN, 18),
])

# get all matching records as a list
all_active = await User.find_all(constraints=[
    constraint("status", ConstraintType.EQUALS, "active"),
])

# iterate through all records with constant memory
async for user in User.find_iter():
    print(user.name)

# update
user.name = "Ada L."
await user.save()

# delete
await user.delete()
```

## Smart Upserts

The `create_or_update` method handles the common "upsert" pattern with configurable strategies for handling duplicates:

```python
from bubble_data_api_client import OnMultiple

# basic upsert, matches by external_id and creates if not found
user, created = await User.create_or_update(
    match={"external_id": "ext-123"},
    data={"name": "Updated Name", "email": "new@example.com"},
    on_multiple=OnMultiple.ERROR,
)
# returns (User, bool): the instance and whether it was created
```

### Duplicate Handling Strategies

Since Bubble doesn't enforce unique constraints, duplicates can occur. Choose how to handle them:

| Strategy | Behavior |
|----------|----------|
| `OnMultiple.ERROR` | Raise `MultipleMatchesError` (fail-fast) |
| `OnMultiple.UPDATE_FIRST` | Update first match (arbitrary order) |
| `OnMultiple.UPDATE_ALL` | Update all matches concurrently |
| `OnMultiple.DEDUPE_OLDEST` | Keep oldest record, delete others, then update |
| `OnMultiple.DEDUPE_NEWEST` | Keep newest record, delete others, then update |

```python
# auto-deduplicate, keeping the oldest record
user, created = await User.create_or_update(
    match={"external_id": "ext-123"},
    data={"name": "Canonical Name"},
    on_multiple=OnMultiple.DEDUPE_OLDEST,
)
```

## Constraints

Build type-safe queries using Bubble's constraint system:

```python
from bubble_data_api_client import constraint, ConstraintType

constraints = [
    constraint("status", ConstraintType.EQUALS, "active"),
    constraint("age", ConstraintType.GREATER_THAN, 21),
    constraint("tags", ConstraintType.CONTAINS, "premium"),
    constraint("email", ConstraintType.IS_NOT_EMPTY),
    constraint("category", ConstraintType.IN, ["A", "B", "C"]),
]

results = await User.find(constraints=constraints)
```

Available constraint types: `EQUALS`, `NOT_EQUAL`, `IS_EMPTY` (any field), `IS_NOT_EMPTY` (any field), `TEXT_CONTAINS`, `NOT_TEXT_CONTAINS`, `GREATER_THAN`, `LESS_THAN`, `IN`, `NOT_IN`, `CONTAINS`, `NOT_CONTAINS`, `EMPTY` (list fields), `NOT_EMPTY` (list fields), `GEOGRAPHIC_SEARCH`.

## Querying Records

Three methods for fetching records, depending on your needs:

| Method | Returns | Use case |
|--------|---------|----------|
| `find()` | `list` | Single page with manual pagination via `cursor`/`limit` |
| `find_all()` | `list` | All matching records collected into memory |
| `find_iter()` | `AsyncIterator` | All matching records with constant memory |

```python
# find(): single page, you control pagination
page1 = await User.find(limit=100)
page2 = await User.find(limit=100, cursor=100)

# find_all(): fetches all pages, returns when complete
all_users = await User.find_all(constraints=[...])
print(f"Got {len(all_users)} users")

# find_iter(): streams records with constant memory
async for user in User.find_iter(constraints=[...]):
    await process(user)  # each record processed as it arrives
```

Both `find_all()` and `find_iter()` handle pagination internally, fetching pages of `page_size` (default 100) until all records are retrieved.

## Type-Safe Bubble UIDs

Validate Bubble record IDs at the type level:

```python
from bubble_data_api_client import BubbleModel, BubbleUID, OptionalBubbleUID, OptionalBubbleUIDs

class Order(BubbleModel, typename="order"):
    customer: BubbleUID                    # required, validated
    referrer: OptionalBubbleUID = None     # optional, coerces invalid to None
    items: OptionalBubbleUIDs = None       # list of UIDs, filters invalid

# validation helpers
from bubble_data_api_client import is_bubble_uid, filter_bubble_uids

is_bubble_uid("1234567890x1234567890")  # True
is_bubble_uid("invalid")                 # False

filter_bubble_uids(["1661531100253x688916634279608300", "invalid", None])  # ["1661531100253x688916634279608300"]
```

## Connection Pooling

Clients are automatically pooled per event loop. For explicit lifecycle control:

```python
from bubble_data_api_client import client_scope, close_clients

# option 1: context manager (auto-closes on exit)
async with client_scope():
    await User.create(name="Test", email="test@example.com")

# option 2: manual cleanup
await close_clients()
```

## Retry Configuration

Plug in custom retry policies using `tenacity`:

```python
import httpx
import tenacity
from bubble_data_api_client import configure

retry_policy = tenacity.AsyncRetrying(
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(httpx.TimeoutException),
)

configure(
    data_api_root_url="https://your-app.bubbleapps.io/api/1.1/obj",
    api_key="your-api-key",
    retry=retry_policy,
)
```

## Usage in Sync Contexts

This library is async-only, but you can use it in sync code:

```python
import asyncio
from bubble_data_api_client import BubbleModel, constraint, ConstraintType

class User(BubbleModel, typename="user"):
    name: str
    email: str
    early_access_enabled: bool = False

# simple scripts
user = asyncio.run(User.get("1234567890x1234567890"))

# or wrap multiple operations
async def main():
    constraints = [
        constraint("is_verified", ConstraintType.EQUALS, True),
        constraint("account_type", ConstraintType.EQUALS, "premium"),
    ]
    users = await User.find(constraints=constraints)
    for user in users:
        user.early_access_enabled = True
        await user.save()

asyncio.run(main())
```

## Error Handling

```python
from bubble_data_api_client import OnMultiple
from bubble_data_api_client.exceptions import (
    BubbleError,              # base exception
    BubbleHttpError,          # HTTP errors
    BubbleUnauthorizedError,  # 401/403 responses
    MultipleMatchesError,     # create_or_update found duplicates (with on_multiple=ERROR)
    PartialFailureError,      # some batch operations failed
    InvalidBubbleUIDError,    # invalid UID format
    ConfigurationError,       # missing configuration
)

# get() returns None if not found
user = await User.get("1661531100253x688916634279608300")
if user is None:
    print("User not found")

# create_or_update raises MultipleMatchesError with on_multiple=ERROR
try:
    user, created = await User.create_or_update(
        match={"external_id": "ext-123"},
        data={"name": "Test"},
        on_multiple=OnMultiple.ERROR,
    )
except MultipleMatchesError as e:
    print(f"Found {e.count} duplicates for {e.match}")
```

## License

MIT
