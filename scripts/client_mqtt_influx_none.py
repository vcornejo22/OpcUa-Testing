import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from aiomqtt import Client as MQTTClient, TLSParameters
from aiomqtt.exceptions import MqttError, MqttReentrantError
from asyncua import Client
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
UNS_TOPIC_PREFIX = "uns/prosys_plant"

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

# Clase para manejar conexiones y publicaciones MQTT
class MQTTHandler:
    def __init__(self, config_mqtt, config_sparkplug):
        """
        Inicializa el manejador MQTT con la configuración proporcionada.
        
        Args:
            config (dict): Diccionario con la configuración MQTT
        """
        self.config_mqtt = config_mqtt
        self.config_sparkplug = config_sparkplug
        self.client = None
        self.connected = False
        self.sequence_number = 0
        self.metrics_registry: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Conecta al broker MQTT con configuración TLS"""
        try:
            tls_parameters = TLSParameters(
                ca_certs=self.config_mqtt["cafile"],
                certfile=self.config_mqtt["cert"],
                keyfile=self.config_mqtt["key"]
            )

            self.client = MQTTClient(
                hostname=self.config_mqtt["broker"],
                port=self.config_mqtt["port"],
                username=self.config_mqtt["user"],
                password=self.config_mqtt["password"],
                tls_params=tls_parameters
            )
            
            await self.client.__aenter__()
            self.connected = True
            self.logger.info(f"Conectado al broker MQTT: {self.config_mqtt['broker']}:{self.config_mqtt['port']}")
            
        except Exception as e:
            self.logger.error(f"Error al conectar al broker MQTT: {e}")
            raise
            
    async def disconnect(self):
        """Desconecta del broker MQTT"""
        if self.client and self.connected:
            try:
                await self.client.__aexit__(None, None, None)
                self.connected = False
                self.logger.info("Desconectado del broker MQTT")
            except Exception as e:
                self.logger.error(f"Error al desconectar del broker MQTT: {e}")
    
    async def __aenter__(self):
        """Context manager entry"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        await self.disconnect()
    
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
        
        self.logger.info(f"Publishing birth certificate to {self.config_sparkplug['birth_topic']}")
        await self.client.publish(self.config_sparkplug['birth_topic'], message, qos=1, retain=True)
        
        # Also publish to UNS
        uns_topic = f"{UNS_TOPIC_PREFIX}/birth"
        await self.client.publish(uns_topic, message, qos=1, retain=True)
        
    async def publish_death_certificate(self):
        """Publish death certificate"""
        payload = self.create_death_payload()
        message = json.dumps(payload)
        
        self.logger.info(f"Publishing death certificate to {self.config_sparkplug['death_topic']}")
        await self.client.publish(self.config_sparkplug['death_topic'], message, qos=1, retain=True)
        
        # Also publish to UNS
        uns_topic = f"{UNS_TOPIC_PREFIX}/death"
        await self.client.publish(uns_topic, message, qos=1, retain=True)
                
    async def publish_normal(self, key, node, value, qos=0):
        """
        Publica un mensaje en el broker MQTT.
        
        Args:
            key (str): Clave del nodo (ej: "Constant", "Counter", etc.)
            node (str): Identificador del nodo OPC UA
            value: Valor a publicar
            qos (int): Quality of Service (0, 1, 2)
        """
        if not self.connected or not self.client:
            self.logger.error("Cliente MQTT no conectado")
            return False
        
        topic = f"prosys/{key}/{node}"
        try:
            await self.client.publish(topic, str(value), qos=qos)
            self.logger.info(f"Publicando en MQTT: {topic} - {value}")
            return True
        except (MqttError, MqttReentrantError) as e:
            self.logger.error(f"Error al publicar en MQTT: {e}")
            return False
        
        
    async def publish_data_message(self, metrics: List[Dict[str, Any]]):
        """Publish data message with metrics"""
        payload = self.create_data_payload(metrics)
        message = json.dumps(payload)
        
        # self.logger.info(f"Publishing data message to {self.config_sparkplug['data_topic']}")
        # await self.client.publish(self.config_sparkplug['data_topic'], message, qos=0)
        
        # Estructura UNS
        for metric in metrics:
            uns_topic = f"{UNS_TOPIC_PREFIX}/data/{metric['name']}"
            metric_message = json.dumps(metric)
            await self.client.publish(uns_topic, metric_message, qos=0)
    
    async def publish_sparkplug(self, key, node, value):
        try:
            metric = self.create_metric(f"{key}_{node}", float(value), "Double")
            await self.publish_data_message([metric])
            print(key, node, value)
            # self.logger.info(f"Published Sparkplug B data: {key}_{node} = {value}")
        except (MqttError, MqttReentrantError) as e:
            self.logger.error(f"Error al publicar en MQTT con Sparkplug B: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado al publicar datos Sparkplug B: {e}")

# Clase manejadora de suscripción OPC UA
class SubHandler:
    def __init__(self, write_api, bucket, org, mqtt_client, timeout=60):
        self.last_val = {}
        self.timer_task = {}
        self.write_api = write_api
        self.bucket = bucket
        self.org = org
        self.mqtt_client = mqtt_client
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

            # Guardar nuevo valor en InfluxDB y publica en MQTT
            point = Point("UA").tag("Prosys", key).field(node, val)
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            await self.mqtt_client.publish_sparkplug(key, node, val)

    def event_notification(self, event):
        logger.info("Python: New event", event)

# Función principal
async def main():
    ua_client = Client(url=UA_URL)
    ua_client.application_uri = UA_URI
    
    # Crear instancia del manejador MQTT
    mqtt_handler = MQTTHandler(MQTT_CONFIG, SPARKPLUG_CONFIG)
    
    async with InfluxDBClientAsync(url=INFLUXDB_CONFIG["url"], token=INFLUXDB_CONFIG["token"], org=INFLUXDB_CONFIG["org"]) as influx_client:
        write_api = influx_client.write_api()
            
        try: 
            await ua_client.connect()
            logger.info(f"Conectado al servidor OPC UA: {UA_URL}")

            # Conectar al broker MQTT usando context manager
            async with mqtt_handler as mqtt:
                # Crear lista de nodos a suscribirse
                var_list = [ua_client.get_node(node_id) for node_id in nodes_dict.values()]
                handler = SubHandler(write_api, INFLUXDB_CONFIG["bucket"], INFLUXDB_CONFIG["org"], mqtt)
                sub = await ua_client.create_subscription(1000, handler)
                await sub.subscribe_data_change(var_list)
                
                logger.info("Suscripciones activas. Presiona Ctrl+C para salir.")
                while True:
                    await asyncio.sleep(1)            
                    
        except KeyboardInterrupt:
            logger.info("Finalizando aplicación por interrupción de teclado")
            
        except Exception as e:
            logger.error(f"Error durante la ejecución: {e}")
            
        finally:
            await ua_client.disconnect()
            logger.info("Desconectado del servidor OPC UA")

if __name__ == "__main__":
    asyncio.run(main())