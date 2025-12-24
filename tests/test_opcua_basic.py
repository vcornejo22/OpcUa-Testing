import pytest
from asyncua import Client, Server, ua


@pytest.mark.asyncio
async def test_client_can_read_value_from_local_server(free_port: int) -> None:
    port = free_port
    endpoint = f"opc.tcp://127.0.0.1:{port}/freeopcua/server/"

    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    idx = await server.register_namespace("urn:opcua-testing")

    objects = server.nodes.objects
    test_obj = await objects.add_object(idx, "TestObject")
    test_var = await test_obj.add_variable(idx, "TestValue", 123)
    await test_var.set_writable()

    await server.start()
    try:
        async with Client(url=endpoint) as client:
            node = client.get_node(test_var.nodeid)
            value = await node.read_value()
            assert value == 123

            await node.write_value(ua.Variant(456, ua.VariantType.Int64))
            updated = await node.read_value()
            assert updated == 456
    finally:
        await server.stop()
