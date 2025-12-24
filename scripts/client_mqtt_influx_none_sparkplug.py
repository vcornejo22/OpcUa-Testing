"""
OPC UA to MQTT Bridge with Sparkplug B Standard and UNS Integration

This module implements a bridge between OPC UA servers and MQTT brokers following
the Sparkplug B specification for industrial IoT communication. It also integrates
with a Unified Namespace (UNS) for centralized data management.

Key Features:
- Sparkplug B compliant message format
- Birth and Death certificate management
- UNS topic structure integration
- OPC UA data subscription and forwarding
- InfluxDB data persistence
- TLS-secured MQTT communication

Sparkplug B Topics:
- Birth Certificate: spBv1.0/PROSYS_GROUP/NDATA/OPCUA_EDGE_NODE
- Death Certificate: spBv1.0/PROSYS_GROUP/NDATA/OPCUA_EDGE_NODE  
- Data Messages: spBv1.0/PROSYS_GROUP/DDATA/OPCUA_EDGE_NODE

UNS Topics:
- Birth: uns/prosys/birth
- Death: uns/prosys/death
- Data: uns/prosys/data/{metric_name}

Author: Victor Salvador Cornejo Chicana
Date: 2024
"""

import asyncio
import contextlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiomqtt
from aiomqtt import Client as MQTTClient, TLSParameters
from aiomqtt.exceptions import MqttError, MqttReentrantError
from asyncua import Client, ua
from dotenv import load_dotenv
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
# INFLUXDB Configuración
INFLUXDB_CONFIG = {
    "token": os.getenv("INFLUXDB_TOKEN"),
    "org": os.getenv("INFLUXDB_ORG"),
    "url": os.getenv("INFLUXDB_URL"),
    "bucket": os.getenv("INFLUXDB_BUCKET"),
}

# MQTT Configuración
MQTT_CONFIG = {
    "broker": os.getenv("MQTT_BROKER"),
    "port": int(os.getenv("MQTT_PORT")),
    "cafile": os.getenv("MQTT_CAFILE"),
    "cert": os.getenv("MQTT_CERT"),
    "key": os.getenv("MQTT_KEY"),
    "tls_version": os.getenv("MQTT_TLSV"),
    "user": os.getenv("MQTT_USER"),
    "password": os.getenv("MQTT_PASSWORD"),
}

# OPC UA Configuración
UA_URL="opc.tcp://Victors-MacBook-Pro-2.local:53530/OPCUA/SimulationServer"
UA_URI="urn:Victors-MacBook-Pro-2.local:OPCUA:SimulationServer"

# Sparkplug B Configuration
SPARKPLUG_CONFIG = {
    "group_id": "PROSYS_GROUP",
    "node_id": "OPCUA_BRIDGE",
    "edge_node_id": "OPCUA_EDGE_NODE",
    "namespace": "spBv1.0",  # Sparkplug B version
    "birth_topic": "spBv1.0/PROSYS_GROUP/NDATA/OPCUA_EDGE_NODE",
    "death_topic": "spBv1.0/PROSYS_GROUP/NDATA/OPCUA_EDGE_NODE",
    "data_topic": "spBv1.0/PROSYS_GROUP/DDATA/OPCUA_EDGE_NODE",
    "birth_payload": {
        "timestamp": 0,
        "metrics": [],
        "seq": 0
    }
}

# UNS Topic Structure
UNS_TOPIC_PREFIX = "uns/prosys"

# Nodos del servidor OPC UA
nodes_dict = {
    "Constant": "ns=3;i=1001",
    "Counter": "ns=3;i=1002",
    "Random": "ns=3;i=1003",
    "Sawtooth": "ns=3;i=1004",
    "Sinusoid": "ns=3;i=1005",
    "Square": "ns=3;i=1006",
    "Triangle": "ns=3;i=1007",
}

# Función auxiliar para obtene clave por valor
def get_key_by_value(search_value):
    return next((key for key, value in nodes_dict.items() if value == search_value), None)

# Sparkplug B Message Handler
class SparkplugBHandler:
    def __init__(self, mqtt_client, config: Dict[str, Any]):
        self.mqtt_client = mqtt_client
        self.config = config
        self.sequence_number = 0
        self.metrics_registry: Dict[str, Dict[str, Any]] = {}
        
    def get_timestamp(self) -> int:
        """Get current timestamp in milliseconds"""
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    
    def increment_sequence(self) -> int:
        """Increment and return sequence number"""
        self.sequence_number = (self.sequence_number + 1) % 256
        return self.sequence_number
    
    def create_metric(self, name: str, value: Any, data_type: str = "Double") -> Dict[str, Any]:
        """Create a Sparkplug B metric"""
        metric = {
            "name": name,
            "timestamp": self.get_timestamp(),
            "dataType": data_type,
            "value": value
        }
        
        # Store metric in registry for birth certificate
        self.metrics_registry[name] = metric
        return metric
    
    def create_birth_payload(self) -> Dict[str, Any]:
        """Create birth certificate payload"""
        return {
            "timestamp": self.get_timestamp(),
            "metrics": list(self.metrics_registry.values()),
            "seq": self.increment_sequence()
        }
    
    def create_data_payload(self, metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create data message payload"""
        return {
            "timestamp": self.get_timestamp(),
            "metrics": metrics,
            "seq": self.increment_sequence()
        }
    
    def create_death_payload(self) -> Dict[str, Any]:
        """Create death certificate payload"""
        return {
            "timestamp": self.get_timestamp(),
            "metrics": [],
            "seq": self.increment_sequence()
        }
    
    async def publish_birth_certificate(self):
        """Publish birth certificate"""
        payload = self.create_birth_payload()
        message = json.dumps(payload)
        
        logger.info(f"Publishing birth certificate to {self.config['birth_topic']}")
        await self.mqtt_client.publish(self.config['birth_topic'], message, qos=1, retain=True)
        
        # Also publish to UNS
        uns_topic = f"{UNS_TOPIC_PREFIX}/birth"
        await self.mqtt_client.publish(uns_topic, message, qos=1, retain=True)
    
    async def publish_death_certificate(self):
        """Publish death certificate"""
        payload = self.create_death_payload()
        message = json.dumps(payload)
        
        logger.info(f"Publishing death certificate to {self.config['death_topic']}")
        await self.mqtt_client.publish(self.config['death_topic'], message, qos=1, retain=True)
        
        # Also publish to UNS
        uns_topic = f"{UNS_TOPIC_PREFIX}/death"
        await self.mqtt_client.publish(uns_topic, message, qos=1, retain=True)
    
    async def publish_data_message(self, metrics: List[Dict[str, Any]]):
        """Publish data message with metrics"""
        payload = self.create_data_payload(metrics)
        message = json.dumps(payload)
        
        logger.info(f"Publishing data message to {self.config['data_topic']}")
        await self.mqtt_client.publish(self.config['data_topic'], message, qos=0)
        
        # Also publish to UNS with structured topic
        for metric in metrics:
            uns_topic = f"{UNS_TOPIC_PREFIX}/data/{metric['name']}"
            metric_message = json.dumps(metric)
            await self.mqtt_client.publish(uns_topic, metric_message, qos=0)

# Clase Cliente MQTT
class MQTT:
    def __init__(self, mqtt_client) -> None:
        self.mqtt_client = mqtt_client
        self.connected = False
        
    async def connect(self):
        """Conectar al broker MQTT y manejar reconexiones"""
        while not self.connected:
            try: 
                await self.mqtt_client.__aenter__()
                self.connected = True
                logger.info("Conexión exitosa al broker MQTT")
            except (MqttError, MqttReentrantError) as e:
                logger.error(f"Error conectando al broker MQTT: {e}. Reintentando en 5 segundos...")
                while True: 
                    self.mqtt_client._lock().release()
                    asyncio.sleep(5)

    async def reconnect_if_needed(self):
        """Intentar reconectar si la conexión se pierde"""
        if not self.connected:
            await self.connect()
# Publicar mensaje en MQTT usando Sparkplug B
async def mqtt_publish_sparkplug(key, node, val, sparkplug_handler):
    """Publish data using Sparkplug B format"""
    try:
        # Create metric for this data point
        metric = sparkplug_handler.create_metric(f"{key}_{node}", float(val), "Double")
        
        # Publish as Sparkplug B data message
        await sparkplug_handler.publish_data_message([metric])
        
        logger.info(f"Published Sparkplug B data: {key}_{node} = {val}")
        
    except (MqttError, MqttReentrantError) as e:
        logger.error(f"Error al publicar en MQTT con Sparkplug B: {e}. Iniciando reconexión...")
    except Exception as e:
        logger.error(f"Error inesperado al publicar datos Sparkplug B: {e}")

# Legacy MQTT publish function (kept for compatibility)
async def mqtt_publish(key, node, val, mqtt_client):
    topic = f"prosys/{key}/{node}"
    logger.info(f"Publicando en MQTT: {topic} - {val}")
    try:
        await mqtt_client.publish(topic, str(val), qos=0)
    except (MqttError, MqttReentrantError) as e:
        logger.error(f"Error al publicar en MQTT: {e}. Iniciando reconexión...")
        

# Clase manejadora de suscripción OPC UA
class SubHandler:
    def __init__(self, write_api, bucket, org, mqtt_client, sparkplug_handler, timeout=60):
        self.last_val = {}
        self.timer_task = {}
        self.write_api = write_api
        self.bucket = bucket
        self.org = org
        self.mqtt_client = mqtt_client
        self.sparkplug_handler = sparkplug_handler
        self.timeout = timeout

    async def reset_timer(self, key, node):
        if key in self.timer_task:
            self.timer_task[key].cancel()
        self.timer_task[key] = asyncio.create_task(self.insert_last_value_after_timeout(key, node))

    async def insert_last_value_after_timeout(self, key, node):
        await asyncio.sleep(self.timeout)
        if key in self.last_val:
            logger.info(f"Insertando valor por timeout: {key}")
            point = Point("UA").tag("Prosys", key).field(node, self.last_val[key])
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)

    async def datachange_notification(self, node, val, data):
        key = get_key_by_value(str(node))
        val = float(val)
        if key not in self.last_val or val != self.last_val[key]:
            # Enviar el valor anterior si cambió (solo para "Constant")
            if key == "Constant" and key in self.last_val:
                point = (
                    Point("UA")
                    .tag("Prosys", key)
                    .field(node, self.last_val[key])
                    .time(datetime.now(timezone.utc) - timedelta(seconds=1))
                )
                await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
                logger.info(f"Último valor: {self.last_val}")

            self.last_val[key] = val
            await self.reset_timer(key, node)

            # Guardar nuevo valor en InfluxDB
            point = Point("UA").tag("Prosys", key).field(node, val)
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            
            # Publicar usando Sparkplug B format
            await mqtt_publish_sparkplug(key, node, val, self.sparkplug_handler)

    def event_notification(self, event):
        logger.info("Python: New event", event)

# Función principal
async def main():
    ua_client = Client(url=UA_URL)
    ua_client.application_uri = UA_URI
    
    # Configuración TLS para MQTT
    tls_parameters = TLSParameters(
        ca_certs=MQTT_CONFIG["cafile"],
        certfile=MQTT_CONFIG["cert"],
        keyfile=MQTT_CONFIG["key"]
    )

    mqtt_client = MQTTClient(
        hostname=MQTT_CONFIG["broker"],
        port=MQTT_CONFIG["port"],
        username=MQTT_CONFIG["user"],
        password=MQTT_CONFIG["password"],
        tls_params=tls_parameters
    ) 
    
    # 
    
    async with InfluxDBClientAsync(url=INFLUXDB_CONFIG["url"], token=INFLUXDB_CONFIG["token"], org=INFLUXDB_CONFIG["org"]) as influx_client:
        write_api = influx_client.write_api()
        
        # Initialize Sparkplug B handler
        sparkplug_handler = SparkplugBHandler(mqtt_client, SPARKPLUG_CONFIG)
            
        try: 
            await ua_client.connect()
            logger.info(f"Conectado al servidor OPC UA: {UA_URL}")

            await mqtt_client.__aenter__()  # Realizar la conexión
            logger.info("Conectado al broker MQTT")
            
            # Initialize metrics registry with OPC UA nodes
            for key in nodes_dict.keys():
                sparkplug_handler.create_metric(f"{key}_status", 0, "Int32")
                sparkplug_handler.create_metric(f"{key}_value", 0.0, "Double")
            
            # Publish birth certificate
            await sparkplug_handler.publish_birth_certificate()
            logger.info("Birth certificate published")
            
            # Crear lista de nodos a suscribirse
            var_list = [ua_client.get_node(node_id) for node_id in nodes_dict.values()]
            handler = SubHandler(write_api, INFLUXDB_CONFIG["bucket"], INFLUXDB_CONFIG["org"], mqtt_client, sparkplug_handler)
            sub = await ua_client.create_subscription(1000, handler)
            await sub.subscribe_data_change(var_list)
            logger.info("Subscribed to OPC UA data changes")
        
            while True:
                await asyncio.sleep(1)            
                    
        except KeyboardInterrupt:
            logger.info("Finalizando aplicación por interrupción de teclado")
            
        except Exception as e:
            logger.error(f"Error durante la ejecución: {e}")
            
        finally:
            # Publish death certificate before disconnecting
            try:
                await sparkplug_handler.publish_death_certificate()
                logger.info("Death certificate published")
            except Exception as e:
                logger.error(f"Error publishing death certificate: {e}")
                
            await ua_client.disconnect()
            await mqtt_client.__aexit__(None, None, None)
            logger.info("Desconectado del servidor OPC UA y MQTT")

if __name__ == "__main__":
    asyncio.run(main())