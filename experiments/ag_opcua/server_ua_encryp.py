import asyncio
import logging
import math
import random
import socket
import sys
from datetime import datetime
from pathlib import Path

from asyncua import Server, ua
from asyncua.common.methods import uamethod
from asyncua.crypto.cert_gen import setup_self_signed_certificate
from asyncua.crypto.permission_rules import SimpleRoleRuleset
from asyncua.crypto.truststore import TrustStore
from asyncua.crypto.validator import CertificateValidator, CertificateValidatorOptions
from asyncua.server.user_managers import CertificateUserManager, PermissiveUserManager
from cryptography.x509.oid import ExtendedKeyUsageOID

sys.path.insert(0, "..")
USE_TRUST_STORE = False

class NodeFactory:
    def __init__(self, idx, server):
        self.idx = idx
        self.server = server

    async def create_node(self, name, initial_value, update_period, update_function, *args, **kwargs):
        node = OPCUANode(name, initial_value, update_period, update_function, *args, **kwargs)
        objects = self.server.nodes.objects
        node.node = await objects.add_variable(self.idx, node.name, ua.Variant(node.value, ua.VariantType.Float))
        await node.node.set_writable()
        return node

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
        cert_base = Path(__file__).parent
        server_cert = Path(cert_base / "certificates_server/server-certificate-example.der")
        server_private_key = Path(cert_base / "certificates_server/server-private-key-example.pem")

        host_name = socket.gethostname()
        server_app_uri =   f"myselfsignedserver@{host_name}"
        cert_user_manager = CertificateUserManager()
        await cert_user_manager.add_user("certificates_server/peer-certificate-example-1.der", name='test_user')
    
        self.server.user_manager = cert_user_manager
        
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.server_name)
        self.idx = await self.server.register_namespace(self.uri)
        self.server.set_security_policy([ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
                               permission_ruleset=SimpleRoleRuleset())
        await setup_self_signed_certificate(server_private_key,
                                        server_cert,
                                        server_app_uri,
                                        host_name,
                                        [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH],
                                        {
                                            'countryName': 'PE',
                                            'stateOrProvinceName': 'Arequipa',
                                            'localityName': 'Hunter',
                                            'organizationName': "Sacha",
                                        })
        
        await self.server.load_certificate(str(server_cert))
        await self.server.load_private_key(str(server_private_key))
        if USE_TRUST_STORE:
            trust_store = TrustStore([Path('certificates_server/trusted/certs')], [])
            await trust_store.load()
            validator = CertificateValidator(options=CertificateValidatorOptions.TRUSTED_VALIDATION | CertificateValidatorOptions.PEER_CLIENT,
                                            trust_store = trust_store)
        else:
            validator = CertificateValidator(options=CertificateValidatorOptions.EXT_VALIDATION | CertificateValidatorOptions.PEER_CLIENT)
        self.server.set_certificate_validator(validator)

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

    async def create_nodes(self, node_factory, num_nodes_per_function):
        update_functions = [linear_update, random_update, sawtooth_update, sinusoid_update, square_update, triangle_update]
        for func in update_functions:
            for i in range(num_nodes_per_function):
                name = f"{func.__name__}Node{i}"
                initial_value = random.random()
                update_period = random.uniform(0.1, 2)  # Update period between 0.1 and 2 seconds
                args = (10, 0, 10)  # Example args, modify as needed for each function
                node = await node_factory.create_node(name, initial_value, update_period, func, *args)
                self.nodes.append(node)

    async def start(self):
        await self.register_method()
        async with self.server:
            update_tasks = [asyncio.create_task(node.start_updating()) for node in self.nodes]
            await asyncio.gather(*update_tasks)

def linear_update(value,  *args, **kwargs):
    return value + 0.1

def random_update(value,  *args, **kwargs):
    return random.random()

def sawtooth_update(value, period=10, lower_limit=0, upper_limit=10,  *args, **kwargs):
    value = (value + 1) % period
    return lower_limit + (upper_limit - lower_limit) * (value / period)

def sinusoid_update(value, amplitude=1, frequency=1, phase=0, lower_limit=0, upper_limit=1,  *args, **kwargs):
    timestamp = datetime.now().timestamp()
    sinusoid_value = amplitude * math.sin(2 * math.pi * frequency * timestamp + phase)
    return lower_limit + (upper_limit - lower_limit) * ((sinusoid_value + amplitude) / (2 * amplitude))

def square_update(value, period=10, lower_limit=0, upper_limit=1,  *args, **kwargs):
    timestamp = datetime.now().timestamp()
    if int(timestamp) % period < period / 2:
        return upper_limit
    else:
        return lower_limit

def triangle_update(value, period=10, lower_limit=0, upper_limit=1,  *args, **kwargs):
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

    # Crear fábrica de nodos
    node_factory = NodeFactory(opcua_server.idx, opcua_server.server)
    
    # Número total de nodos deseado
    total_nodes = 1000
    num_nodes_per_function = 6
    num_nodes_per_function = total_nodes // num_nodes_per_function
    
    await opcua_server.create_nodes(node_factory, num_nodes_per_function)
    
    _logger.info("Starting server!")
    # Iniciar el servidor
    await opcua_server.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(), debug=True)
