"""Python client for Bubble.io Data API with ORM-style models and async support.

This library provides two ways to interact with Bubble's Data API:
- BubbleModel: ORM-style base class for defining typed data models with CRUD operations
- RawClient: Low-level async client for direct API access

Quick start:
    1. Configure credentials: configure(data_api_root_url="...", api_key="...")
    2. Define a model: class User(BubbleModel, typename="user"): name: str
    3. Use CRUD operations: await User.create(name="Alice")
"""

from bubble_data_api_client.client.orm import BubbleModel
from bubble_data_api_client.client.raw_client import RawClient
from bubble_data_api_client.config import (
    BubbleConfig,
    ConfigProvider,
    configure,
    set_config_provider,
)
from bubble_data_api_client.constraints import Constraint, ConstraintType, constraint
from bubble_data_api_client.exceptions import BubbleAPIError
from bubble_data_api_client.pool import client_scope, close_clients
from bubble_data_api_client.types import (
    BubbleField,
    BubbleUID,
    BulkCreateItemResult,
    OnMultiple,
    OptionalBubbleUID,
    OptionalBubbleUIDs,
)
from bubble_data_api_client.validation import filter_bubble_uids, is_bubble_uid

__all__ = [
    # config
    "BubbleConfig",
    "ConfigProvider",
    "configure",
    "set_config_provider",
    # client classes
    "BubbleModel",
    "RawClient",
    # exceptions
    "BubbleAPIError",
    # query building
    "Constraint",
    "ConstraintType",
    "constraint",
    # client lifecycle
    "client_scope",
    "close_clients",
    # types
    "BubbleField",
    "BubbleUID",
    "BulkCreateItemResult",
    "OnMultiple",
    "OptionalBubbleUID",
    "OptionalBubbleUIDs",
    # validation
    "filter_bubble_uids",
    "is_bubble_uid",
]
