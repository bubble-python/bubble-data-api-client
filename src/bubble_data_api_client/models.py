from datetime import datetime

from pydantic import BaseModel, Field

from bubble_data_api_client.types import BubbleField


class BubbleThing(BaseModel):
    """
    Built-in fields for Bubble Things.
    https://manual.bubble.io/help-guides/data/the-database/data-types-and-fields#built-in-fields
    """

    id: str = Field(
        ...,
        alias=BubbleField.ID,
        description="The Unique ID in format '{timestamp}x{random}' that identifies a specific thing in the database.",
    )
    created_date: datetime = Field(
        ...,
        alias=BubbleField.CREATED_DATE,
        description="The creation date of the Bubble Thing. Never changes.",
    )
    modified_date: datetime = Field(
        ...,
        alias=BubbleField.MODIFIED_DATE,
        description="Automatically updated any time any changes are made to the Thing.",
    )
    slug: str = Field(
        ...,
        alias=BubbleField.SLUG,
        description="A user-friendly and search engine optimized URL of the Bubble Thing.",
    )
