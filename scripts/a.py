import asyncio
from asyncua import Client, ua
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
from aiomqtt import TLSParameters
from aiomqtt import Client as MQTTClient
import aiomqtt
import logging

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
UA_URL="opc.tcp://Victors-MacBook-Pro.local:53530/OPCUA/SimulationServer"
UA_URI="urn:Victors-MacBook-Pro.local:OPCUA:SimulationServer"

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

# Función auxiliar para obtener clave por valor
def get_key_by_value(search_value):
    return next((key for key, value in nodes_dict.items() if value == search_value), None)

# Función de reconexión MQTT en segundo plano
async def mqtt_reconnect(mqtt_client, interval=5):
    while True:
        try:
            logger.info(f"Intentando reconectar al broker MQTT en {interval} segundos...")
            await mqtt_client.__aenter__()  # Intentar reconectar
            logger.info("Reconexión exitosa al broker MQTT.")
            break  # Si la reconexión es exitosa, salir del bucle
        except aiomqtt.MqttError as e:
            logger.error(f"Error al reconectar al broker MQTT: {e}. Reintentando en {interval} segundos...")
            await asyncio.sleep(interval)

# Publicar mensaje en MQTT
async def mqtt_publish(key, node, val, mqtt_client):
    topic = f"prosys/{key}/{node}"
    logger.info(f"Publicando en MQTT: {topic} - {val}")
    try:
        await mqtt_client.publish(topic, str(val), qos=0)
    except aiomqtt.MqttError as e:
        logger.error(f"Conexión pérdida. {e}")
        # Iniciar tarea de reconexión en segundo plano sin bloquear la ejecución
        asyncio.create_task(mqtt_reconnect(mqtt_client))

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

            # Guardar nuevo valor en InfluxDB y publicar en MQTT
            point = Point("UA").tag("Prosys", key).field(node, val)
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            await mqtt_publish(key, node, val, self.mqtt_client)

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
    async with InfluxDBClientAsync(url=INFLUXDB_CONFIG["url"], token=INFLUXDB_CONFIG["token"], org=INFLUXDB_CONFIG["org"]) as influx_client:
        
        write_api = influx_client.write_api()
            
        try: 
            await ua_client.connect()
            logger.info(f"Conectado al servidor OPC UA: {UA_URL}")
            logger.info("Nodos root hijos son: %s", await ua_client.nodes.root.get_children())
            await mqtt_client.__aenter__()  # Realizar la conexión inicial
            # Crear lista de nodos a suscribirse
            var_list = [ua_client.get_node(node_id) for node_id in nodes_dict.values()]
            handler = SubHandler(write_api, INFLUXDB_CONFIG["bucket"], INFLUXDB_CONFIG["org"], mqtt_client)
            sub = await ua_client.create_subscription(1000, handler)
            await sub.subscribe_data_change(var_list)
        
            while True:
                await asyncio.sleep(1)            
                    
        except KeyboardInterrupt:
            logger.info("Finalizando aplicación por interrupción de teclado")
            
        except Exception as e:
            logger.error(f"Error durante la ejecución: {e}")
            
        finally:
            await ua_client.disconnect()
            await mqtt_client.__aexit__(None, None, None)
            logger.info("Desconectado del servidor OPC UA")

if __name__ == "__main__":
    asyncio.run(main())
