import asyncio

from asyncua import Client, ua
from asyncua.common.subscription import Subscription


class OPCUAClient:
    def __init__(self, url):
        self.url = url
        self.client = Client(url)
        self.sub = None

    async def connect(self):
        await self.client.connect()
        print("Conectado al servidor OPC UA")

    async def disconnect(self):
        await self.client.disconnect()
        print("Desconectado del servidor OPC UA")

    async def get_server_info(self):
        server_info = await self.client.get_server_info()
        print("Información del servidor:", server_info)

    async def read_node_value(self, node_id):
        node = self.client.get_node(node_id)
        value = await node.read_value()
        print(f"Valor del nodo {node_id}: {value}")
        return value

    async def write_node_value(self, node_id, value):
        node = self.client.get_node(node_id)
        await node.write_value(value)
        print(f"Valor escrito en el nodo {node_id}: {value}")

    async def subscribe_to_node(self, node_id, callback):
        handler = SubHandler(callback)
        self.sub = await self.client.create_subscription(1000, handler)
        node = self.client.get_node(node_id)
        handle = await self.sub.subscribe_data_change(node)
        print(f"Suscrito al nodo {node_id}")
        return handle

    async def unsubscribe(self, handle):
        if self.sub:
            await self.sub.unsubscribe(handle)
            print("Desuscrito del nodo")

class SubHandler:
    def __init__(self, callback):
        self.callback = callback

    def datachange_notification(self, node, val, data):
        print(f"Notificación de cambio de datos: Nodo {node}, Valor {val}")
        self.callback(node, val, data)

    def event_notification(self, event):
        print(f"Notificación de evento: {event}")

async def main():
    url = "opc.tcp://localhost:4840/freeopcua/server/"
    client = OPCUAClient(url)
    
    await client.connect()
    await client.get_server_info()

    node_id = "ns=2;i=2"
    await client.read_node_value(node_id)
    await client.write_node_value(node_id, 42)

    async def data_change_callback(node, val, data):
        print(f"Callback - Nodo: {node}, Valor: {val}, Datos: {data}")

    handle = await client.subscribe_to_node(node_id, data_change_callback)
    await asyncio.sleep(10)
    await client.unsubscribe(handle)

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
