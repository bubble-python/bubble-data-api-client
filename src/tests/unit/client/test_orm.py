from bubble_data_api_client.client.orm import BubbleBaseModel


def test_model_instantiation():
    """Tests that the Pydantic model can be instantiated."""

    class User(BubbleBaseModel, typename="user"):
        name: str

    # instantiate the model, no client is needed
    user = User(name="testuser", _id="12345")

    assert user.uid == "12345"
    assert user.name == "testuser"
