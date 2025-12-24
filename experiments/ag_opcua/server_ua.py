import asyncio
import logging
import math
import random
from datetime import datetime

from asyncua import Server, ua
from asyncua.common.methods import uamethod


class OPCUANode:
    def __init__(self, name, initial_value, update_period, update_function, *args, **kwargs):
        self.name = name
        self.value = initial_value
        self.update_period = update_period
        self.update_function = update_function
        self.node = None
        self.args = args
        self.kwargs = kwargs

    async def start_updating(self):
        while True:
            await asyncio.sleep(self.update_period)
            new_value = self.update_function(self.value, *self.args, **self.kwargs)
            await self.node.write_value(new_value, ua.VariantType.Float)
            self.value = new_value

class OPCUAServer:
    def __init__(self, endpoint, server_name, uri):
        self.server = Server()
        self.endpoint = endpoint
        self.server_name = server_name
        self.uri = uri
        self.nodes = []

    async def init_server(self):
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.server_name)
        self.idx = await self.server.register_namespace(self.uri)

    @uamethod
    def func(parent, value):
        return value * 2

    async def register_method(self):
        await self.server.nodes.objects.add_method(
            ua.NodeId("ServerMethod", self.idx),
            ua.QualifiedName("ServerMethod", self.idx),
            self.func,
            [ua.VariantType.Int64],
            [ua.VariantType.Int64],
        )

    async def create_node(self, name, initial_value, update_period, update_function, *args, **kwargs):
        node = OPCUANode(name, initial_value, update_period, update_function, *args, **kwargs)
        objects = self.server.nodes.objects
        node.node = await objects.add_variable(self.idx, node.name, ua.Variant(node.value, ua.VariantType.Float))
        await node.node.set_writable()
        self.nodes.append(node)

    async def start(self):
        await self.register_method()
        async with self.server:
            update_tasks = [asyncio.create_task(node.start_updating()) for node in self.nodes]
            await asyncio.gather(*update_tasks)

def linear_update(value):
    return value + 0.1

def random_update(value):
    return random.random()

def sawtooth_update(value, period=10, lower_limit=0, upper_limit=10):
    value = (value + 1) % period
    return lower_limit + (upper_limit - lower_limit) * (value / period)

def sinusoid_update(value, amplitude=1, frequency=1, phase=0, lower_limit=0, upper_limit=1):
    timestamp = datetime.now().timestamp()
    sinusoid_value = amplitude * math.sin(2 * math.pi * frequency * timestamp + phase)
    return lower_limit + (upper_limit - lower_limit) * ((sinusoid_value + amplitude) / (2 * amplitude))

def square_update(value, period=10, lower_limit=0, upper_limit=1):
    timestamp = datetime.now().timestamp()
    if int(timestamp) % period < period / 2:
        return upper_limit
    else:
        return lower_limit

def triangle_update(value, period=10, lower_limit=0, upper_limit=1):
    timestamp = datetime.now().timestamp()
    phase = int(timestamp) % period
    if phase < period / 2:
        return lower_limit + (upper_limit - lower_limit) * (2 * phase / period)
    else:
        return upper_limit - (upper_limit - lower_limit) * (2 * (phase - period / 2) / period)

async def main():
    _logger = logging.getLogger(__name__)
    opcua_server = OPCUAServer("opc.tcp://0.0.0.0:4840/freeopcua/server/", "Asyncua Example Server", "http://examples.freeopcua.github.io")

    await opcua_server.init_server()

    # Crear nodos en el servidor con diferentes funciones de actualización
    await opcua_server.create_node("LinearNode", 0.0, 2, linear_update)
    await opcua_server.create_node("RandomNode", 0.0, 1, random_update)
    await opcua_server.create_node("SawtoothNode", 0.0, 1, sawtooth_update, 10, 0, 10)
    await opcua_server.create_node("SinusoidNode", 0.0, 1, sinusoid_update, 1, 1, 0, -1, 1)
    await opcua_server.create_node("SquareNode", 0.0, 1, square_update, 10, 0, 1)
    await opcua_server.create_node("TriangleNode", 0.0, 1, triangle_update, 10, 0, 1)

    _logger.info("Starting server!")

    # Iniciar el servidor
    await opcua_server.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(), debug=True)
