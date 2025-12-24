from bubble_data_api_client.client import raw_client


async def test_raw_client_init() -> None:
    """Test that RawClient can be instantiated and used as context manager."""
    # test creating an instance
    client = raw_client.RawClient()
    assert isinstance(client, raw_client.RawClient)

    # test async context manager
    async with client as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)

    # test creating with async context manager
    async with raw_client.RawClient() as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)
