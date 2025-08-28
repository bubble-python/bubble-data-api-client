import pytest

from bubble_data_api_client.client.orm import BubbleBaseModel


class IntegrationTestModel(BubbleBaseModel, typename="IntegrationTest"):
    text: str


@pytest.mark.asyncio
async def test_orm_integration():
    """Integration test for the ORM layer."""

    # create
    thing = await IntegrationTestModel.create(text="test")
    assert isinstance(thing, IntegrationTestModel)
    assert thing.text == "test"
    assert thing.uid is not None

    # get
    thing2 = await IntegrationTestModel.get(thing.uid)
    assert isinstance(thing2, IntegrationTestModel)
    assert thing2.text == "test"
    assert thing2.uid == thing.uid

    # update
    thing2.text = "test2"
    await thing2.save()

    # get again
    thing3 = await IntegrationTestModel.get(thing.uid)
    assert isinstance(thing3, IntegrationTestModel)
    assert thing3.text == "test2"

    # list
    things = await IntegrationTestModel.list(
        constraints=[{"key": "_id", "constraint_type": "equals", "value": thing3.uid}]
    )
    assert len(things) == 1
    assert isinstance(things[0], IntegrationTestModel)
    assert things[0].uid == thing3.uid
    assert things[0].text == "test2"

    # delete
    await thing3.delete()

    # verify deletion
    thing4 = await IntegrationTestModel.get(thing.uid)
    assert thing4 is None
