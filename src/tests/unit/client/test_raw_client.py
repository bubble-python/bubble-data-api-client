from bubble_data_api_client.client import raw_client


async def test_raw_client_init(test_url: str, test_api_key: str):
    """Test that client is instantiated."""

    # test creating an instance
    client = raw_client.RawClient(
        data_api_root_url=test_url,
        api_key=test_api_key,
    )
    assert isinstance(client, raw_client.RawClient)

    # test async context manager
    async with client as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)

    # test creating with async context manager
    async with raw_client.RawClient(
        data_api_root_url=test_url,
        api_key=test_api_key,
    ) as client_instance:
        assert isinstance(client_instance, raw_client.RawClient)
