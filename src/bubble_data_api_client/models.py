from datetime import datetime

from pydantic import BaseModel, Field


class BubbleThing(BaseModel):
    """
    Built-in fields for Bubble Things.
    https://manual.bubble.io/help-guides/data/the-database/data-types-and-fields#built-in-fields
    """

    id: str = Field(
        ...,
        alias="_id",
        description="The Unique ID is a 32-character alphanumeric string "
        "that identifies a specific thing in the database.",
    )
    created_date: datetime = Field(
        ...,
        alias="Created Date",
        description="The creation date of the Bubble Thing. Never changes.",
    )
    modified_date: datetime = Field(
        ...,
        alias="Modified Date",
        description="Automatically updated any time any changes are made to the Thing.",
    )
    slug: str = Field(
        ...,
        alias="Slug",
        description="A user-friendly and search engine optimized URL of the Bubble Thing.",
    )
