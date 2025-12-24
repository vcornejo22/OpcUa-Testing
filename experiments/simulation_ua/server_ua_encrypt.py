import asyncio
import logging
import math
import random
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
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

    async def create_node(
        self, name, initial_value, update_period, update_function, *args, **kwargs
    ):
        node = OPCUANode(
            name, initial_value, update_period, update_function, *args, **kwargs
        )
        objects = self.server.nodes.objects
        node.node = await objects.add_variable(
            self.idx, node.name, ua.Variant(node.value, ua.VariantType.Float)
        )
        await node.node.set_writable()
        return node

class OPCUANode:
    def __init__(
        self, name, initial_value, update_period, update_function, *args, **kwargs
    ):
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
            
            # Si la función es una corutina (asincrona), usa await
            if asyncio.iscoroutinefunction(self.update_function):
                new_value = await self.update_function(self.value, *self.args, **self.kwargs)
            else:
                new_value = self.update_function(self.value, *self.args, **self.kwargs)
                
            if not isinstance(new_value, float):
                new_value = float(new_value) if new_value is not None else 0.0
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
        
        rsa_bits = [1024, 2048, 3072, 4096]
        signature_algorithms = ["Sha1", "Sha224", "Sha256", "Sha384", "Sha512"]
        list_certs = [f"{cert_base}/opcua/{x}_{y}/{x}_{y}_certificate.pem" for x in rsa_bits for y in signature_algorithms]
        list_private_key = [f"{cert_base}/opcua/{x}_{y}/{x}_{y}_private_key.pem" for x in rsa_bits for y in signature_algorithms]
        
        server_cert = Path(cert_base / "certs/cert-ics.der")
        server_private_key = Path(cert_base / "certs/key-ics.pem")
        host_name = socket.gethostname()
        server_app_uri = f"myselfsignedserver@{host_name}"
        cert_user_manager = CertificateUserManager()
        # await cert_user_manager.add_user(
        #     "certificates_server/peer-certificate-example-1.der", name="test_user"
        # )

        self.server.user_manager = cert_user_manager

        await self.server.init()
        await self.server.set_application_uri(server_app_uri)
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.server_name)
        self.idx = await self.server.register_namespace(self.uri)
        self.server.set_security_policy(
            [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
            permission_ruleset=SimpleRoleRuleset(),
        )
        
        await setup_self_signed_certificate(
                server_private_key,
                server_cert,
                server_app_uri,
                host_name,
                [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH],
                {
                    "countryName": "PE",
                    "stateOrProvinceName": "Arequipa",
                    "localityName": "Hunter",
                    "organizationName": "Sacha",
                },
            )
        
        for i in range(len(list_certs)):
            await self.server.load_certificate(str(list_certs[i]))
            await self.server.load_private_key(str(list_private_key[i]))
            
        if USE_TRUST_STORE:
            trust_store = TrustStore([Path("certificates_server/trusted/certs")], [])
            await trust_store.load()
            validator = CertificateValidator(
                options=CertificateValidatorOptions.TRUSTED_VALIDATION
                | CertificateValidatorOptions.PEER_CLIENT,
                trust_store=trust_store,
            )
        else:
            validator = CertificateValidator(
                options=CertificateValidatorOptions.EXT_VALIDATION
                | CertificateValidatorOptions.PEER_CLIENT
            )
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

    async def create_nodes_in_batches(self, node_factory, total_nodes, batch_size=50, batch_delay=0.5):
        """
        Crea nodos en baches para cada función de actualización en paralelo y luego los combina al final.
        
        :param node_factory: La fábrica de nodos que crea los nodos OPC UA.
        :param total_nodes: Número total de nodos a crear.
        :param batch_size: Tamaño del lote de nodos a crear por vez.
        :param batch_delay: Pausa entre la creación de cada lote, en segundos.
        """
        update_functions = [
            linear_update,
            random_update,
            sawtooth_update,
            sinusoid_update,
            square_update,
            triangle_update,
            exponential_decay_update,
            logarithmic_growth_update,
            brownian_update,
            step_update,
            pulse_update,
            noisy_update,
            sigmoid_update,
            random_walk_update,
            custom_oscillation_update,
            poisson_update,
        ]
        
        len_functions = len(update_functions)
        num_nodes_per_function = total_nodes // len_functions
        
        # Función auxiliar para crear nodos de una función en baches
        async def create_batch_for_function(func):
            nodes = []
            nodes_created = 0
            while nodes_created < num_nodes_per_function:
                current_batch_size = min(batch_size, num_nodes_per_function - nodes_created)
                
                for _ in range(current_batch_size):
                    name = f"{func.__name__}Node{nodes_created}"
                    initial_value = random.random()
                    update_period = random.uniform(0.5, 2)  # Intervalo de actualización
                    
                    # Crear nodo
                    node = await node_factory.create_node(
                        name, initial_value, update_period, func
                    )
                    nodes.append(node)
                    nodes_created += 1
                
                # Pausa entre baches
                await asyncio.sleep(batch_delay)
            return nodes

        # Crear tareas paralelas para cada función de actualización
        tasks = [create_batch_for_function(func) for func in update_functions]
        
        # Esperar a que todas las tareas terminen
        results = await asyncio.gather(*tasks)
        
        # Agregar los nodos creados de cada tarea a self.nodes
        for node_list in results:
            self.nodes.extend(node_list)


            
    async def start(self):
        await self.register_method()
        async with self.server:
            update_tasks = [
                asyncio.create_task(node.start_updating()) for node in self.nodes
            ]
            await asyncio.gather(*update_tasks)

## Funciones para simular datos
def linear_update(value, *args, **kwargs):
    return value + 0.1

def random_update(value, *args, **kwargs):
    return random.random()

def sawtooth_update(value, period=10, lower_limit=0, upper_limit=10, *args, **kwargs):
    value = (value + 1) % period
    return lower_limit + (upper_limit - lower_limit) * (value / period)

def sinusoid_update(
    value,
    amplitude=1,
    frequency=1,
    phase=0,
    lower_limit=0,
    upper_limit=1,
    *args,
    **kwargs,
):
    timestamp = datetime.now().timestamp()
    sinusoid_value = amplitude * math.sin(2 * math.pi * frequency * timestamp + phase)
    return lower_limit + (upper_limit - lower_limit) * (
        (sinusoid_value + amplitude) / (2 * amplitude)
    )

def square_update(value, period=10, lower_limit=0, upper_limit=1, *args, **kwargs):
    timestamp = datetime.now().timestamp()
    if int(timestamp) % period < period / 2:
        return upper_limit
    else:
        return lower_limit

def triangle_update(value, period=10, lower_limit=0, upper_limit=1, *args, **kwargs):
    timestamp = datetime.now().timestamp()
    phase = int(timestamp) % period
    if phase < period / 2:
        return lower_limit + (upper_limit - lower_limit) * (2 * phase / period)
    else:
        return upper_limit - (upper_limit - lower_limit) * (
            2 * (phase - period / 2) / period
        )

def logarithmic_growth_update(value, growth_rate=0.1, lower_limit=0, upper_limit=10, *args, **kwargs):
    value = math.log(value + growth_rate)
    return max(lower_limit, min(value, upper_limit))

def exponential_decay_update(value, decay_rate=0.1, lower_limit=0, upper_limit=1, *args, **kwargs):
    value = value * (1 - decay_rate)
    return max(lower_limit, min(value, upper_limit))

def brownian_update(value, step_size=0.1, lower_limit=0, upper_limit=1, *args, **kwagrs):
    value += random.uniform(-step_size, step_size)
    return max(lower_limit, min(value, upper_limit))

def step_update(value, step_interval=5, lower_limit=0, upper_limit=10, *args, **kwargs):
    timestamp = datetime.now().timestamp()
    if int(timestamp) % step_interval == 0:
        value = random.uniform(lower_limit, upper_limit)
    return value

def pulse_update(value, period=10, pulse_width=2, lower_limit=0, upper_limit=1, *args, **kwagrs):
    timestamp = datetime.now().timestamp()
    phase = int(timestamp) % period
    if phase < pulse_width:
        return upper_limit
    else:
        return lower_limit
    
def noisy_update(value, noise_level=0.1, lower_limit=0, upper_limit=1, *args, **kwargs):
    noise = random.gauss(0, noise_level)
    value += noise
    return max(lower_limit, min(value, upper_limit))
    
def sigmoid_update(value, growth_rate=0.1, lower_limit=0, upper_limit=1, *args, **kwargs):
    value = 1 / (1 + math.exp(-growth_rate * value))
    return lower_limit + (upper_limit - lower_limit) * value

def random_walk_update(value, step_size=0.1, *args, **kwargs):
    value += random.choice([-step_size, step_size])
    return value

def custom_oscillation_update(value, amplitude=1, frequency=1, offset=0, lower_limit=0, upper_limit=1, *args, **kwargs):
    timestamp = time.time()
    oscillation = amplitude * math.sin(2 * math.pi * frequency * timestamp + offset)
    return lower_limit + (upper_limit - lower_limit) * ((oscillation + amplitude) / (2 * amplitude))

def poisson_update(value, lam=3, lower_limit=0, upper_limit=10, *args, **kwargs):
    event_count = np.random.poisson(lam)
    return max(lower_limit, min(value + event_count, upper_limit))

## Función principal
async def main():
    _logger = logging.getLogger(__name__)
    opcua_server = OPCUAServer(
        "opc.tcp://0.0.0.0:4840/freeopcua/server/",
        "Asyncua Example Server",
        "http://examples.freeopcua.github.io",
    )

    await opcua_server.init_server()

    # Crear fábrica de nodos
    node_factory = NodeFactory(opcua_server.idx, opcua_server.server)

    # Número total de nodos deseado
    total_nodes = 2000
    
    # Crear nodos normales
    await opcua_server.create_nodes_in_batches(node_factory, total_nodes)

    _logger.info("Starting server!")
    # Iniciar el servidor
    await opcua_server.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(), debug=True)
