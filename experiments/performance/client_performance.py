import asyncio

from asyncua import Client, Server, ua

url = "opc.tcp://192.168.1.7:4840/freeopcua/server/"
namespace = "http://example.org/freeopcua/server/"


async def main():

    print(f"Connecting to {url} ...")
    async with Client(url=url) as client:
        # Find the namespace index
        nsidx = await client.get_namespace_index(namespace)
        print(f"Namespace Index for '{namespace}': {nsidx}")

        # Get the variable node for read / write
        sinusoid_var = await client.nodes.root.get_child(
            f"0:Objects/{nsidx}:Object/{nsidx}:Sinusoid"
        )
        # square_var = await client.nodes.root.get_child(
        #     f"0:Objects/{nsidx}:MyObject/{nsidx}:Square"
        # )
        sinusoid = await sinusoid_var.read_value()
        print(f"Value of MyVariable ({sinusoid_var}): {sinusoid}")
        # square = await square_var.read_value()
        # print(f"Value of MyVariable ({sinusoid_var}): {square}")

        # new_value = value - 50
        # print(f"Setting value of MyVariable to {new_value} ...")
        # await var.write_value(new_value)

if __name__ == "__main__":
    asyncio.run(main())