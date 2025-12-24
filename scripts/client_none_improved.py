import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from aiomqtt import TLSParameters
from aiomqtt import Client as MQTTClient
from aiomqtt.exceptions import MqttError, MqttReentrantError
from asyncua.client import Client
from dotenv import load_dotenv
from influxdb_client.client.write.point import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

# Configuracion basica de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

def get_env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta la variable de entorno requerida: {name}")
    return value


# INFLUXDB Configuracion
INFLUXDB_CONFIG: Dict[str, str] = {
    "token": get_env_required("INFLUXDB_TOKEN"),
    "org": get_env_required("INFLUXDB_ORG"),
    "url": get_env_required("INFLUXDB_URL"),
    "bucket": get_env_required("INFLUXDB_BUCKET"),
}

# MQTT Configuracion
MQTT_CONFIG: Dict[str, str | int] = {
    "broker": get_env_required("MQTT_BROKER"),
    "port": int(os.getenv("MQTT_PORT", "1883")),
    "cafile": get_env_required("MQTT_CAFILE"),
    "cert": get_env_required("MQTT_CERT"),
    "key": get_env_required("MQTT_KEY"),
    "tls_version": os.getenv("MQTT_TLSV", ""),
    "user": get_env_required("MQTT_USER"),
    "password": get_env_required("MQTT_PASSWORD"),
}

# OPC UA Configuracion
UA_URL = "opc.tcp://Victors-MacBook-Pro.local:53530/OPCUA/SimulationServer"
UA_URI = "urn:Victors-MacBook-Pro.local:OPCUA:SimulationServer"

# Nodos del servidor OPC UA
NODES_DICT: Dict[str, str] = {
    "Constant": "ns=3;i=1001",
    "Counter": "ns=3;i=1002",
    "Random": "ns=3;i=1003",
    "Sawtooth": "ns=3;i=1004",
    "Sinusoid": "ns=3;i=1005",
    "Square": "ns=3;i=1006",
    "Triangle": "ns=3;i=1007",
}


def get_key_by_value(search_value: str) -> Optional[str]:
    return next((key for key, value in NODES_DICT.items() if value == search_value), None)


class MQTT:
    def __init__(self, mqtt_client: MQTTClient) -> None:
        self.mqtt_client = mqtt_client
        self.connected = False

    async def connect(self) -> None:
        """Conectar al broker MQTT y manejar reconexiones."""
        while not self.connected:
            try:
                await self.mqtt_client.__aenter__()
                self.connected = True
                logger.info("Conexion exitosa al broker MQTT")
            except (MqttError, MqttReentrantError) as exc:
                logger.error(
                    "Error conectando al broker MQTT: %s. Reintentando en 5 segundos...",
                    exc,
                )
                await asyncio.sleep(5)

    async def reconnect_if_needed(self) -> None:
        """Intentar reconectar si la conexion se pierde."""
        if not self.connected:
            await self.connect()


async def mqtt_publish(
    key: str,
    node: str,
    val: float,
    mqtt_client: MQTTClient,
    reconnector: MQTT,
) -> None:
    topic = f"prosys/{key}/{node}"
    logger.info("Publicando en MQTT: %s - %s", topic, val)
    while True:
        try:
            await mqtt_client.publish(topic, str(val), qos=0)
            return
        except (MqttError, MqttReentrantError) as exc:
            logger.error("Error al publicar en MQTT: %s. Iniciando reconexion...", exc)
            reconnector.connected = False
            await reconnector.reconnect_if_needed()


class SubHandler:
    def __init__(
        self,
        write_api: Any,
        bucket: str,
        org: str,
        mqtt_client: MQTTClient,
        reconnector: MQTT,
        timeout: int = 60,
    ) -> None:
        self.last_val: Dict[str, float] = {}
        self.timer_task: Dict[str, asyncio.Task[None]] = {}
        self.write_api = write_api
        self.bucket = bucket
        self.org = org
        self.mqtt_client = mqtt_client
        self.reconnector = reconnector
        self.timeout = timeout

    async def reset_timer(self, key: str, node: str) -> None:
        if key in self.timer_task:
            self.timer_task[key].cancel()
        self.timer_task[key] = asyncio.create_task(
            self.insert_last_value_after_timeout(key, node)
        )

    async def insert_last_value_after_timeout(self, key: str, node: str) -> None:
        await asyncio.sleep(self.timeout)
        if key in self.last_val:
            logger.info("Insertando valor por timeout: %s", key)
            point = Point("UA").tag("Prosys", key).field(node, self.last_val[key])
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)

    async def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        del data
        key = get_key_by_value(str(node))
        if key is None:
            logger.debug("Nodo no registrado: %s", node)
            return
        try:
            value = float(val)
        except (TypeError, ValueError):
            logger.warning("Valor no numerico recibido para %s: %s", key, val)
            return

        if key not in self.last_val or value != self.last_val[key]:
            if key == "Constant" and key in self.last_val:
                point = (
                    Point("UA")
                    .tag("Prosys", key)
                    .field(node, self.last_val[key])
                    .time(datetime.now(timezone.utc) - timedelta(seconds=1))
                )
                await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
                logger.info("Ultimo valor: %s", self.last_val)

            self.last_val[key] = value
            await self.reset_timer(key, node)

            point = Point("UA").tag("Prosys", key).field(node, value)
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            await mqtt_publish(key, node, value, self.mqtt_client, self.reconnector)

    def event_notification(self, event: Any) -> None:
        logger.info("Python: New event %s", event)


async def main() -> None:
    ua_client = Client(url=UA_URL)
    ua_client.application_uri = UA_URI

    tls_parameters = TLSParameters(
        ca_certs=MQTT_CONFIG["cafile"],
        certfile=MQTT_CONFIG["cert"],
        keyfile=MQTT_CONFIG["key"],
    )

    mqtt_client = MQTTClient(
        hostname=MQTT_CONFIG["broker"],
        port=MQTT_CONFIG["port"],
        username=MQTT_CONFIG["user"],
        password=MQTT_CONFIG["password"],
        tls_params=tls_parameters,
    )

    reconnector = MQTT(mqtt_client)
    async with InfluxDBClientAsync(
        url=INFLUXDB_CONFIG["url"],
        token=INFLUXDB_CONFIG["token"],
        org=INFLUXDB_CONFIG["org"],
    ) as influx_client:
        write_api = influx_client.write_api()

        try:
            await ua_client.connect()
            logger.info("Conectado al servidor OPC UA: %s", UA_URL)
            logger.info(
                "Nodos root hijos son: %s",
                await ua_client.nodes.root.get_children(),
            )
            await mqtt_client.__aenter__()
            var_list = [ua_client.get_node(node_id) for node_id in NODES_DICT.values()]
            handler = SubHandler(
                write_api,
                INFLUXDB_CONFIG["bucket"],
                INFLUXDB_CONFIG["org"],
                mqtt_client,
                reconnector,
            )
            sub = await ua_client.create_subscription(1000, handler)
            await sub.subscribe_data_change(var_list)

            while True:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("Finalizando aplicacion por interrupcion de teclado")
        except Exception as exc:
            logger.error("Error durante la ejecucion: %s", exc)
        finally:
            await ua_client.disconnect()
            await mqtt_client.__aexit__(None, None, None)
            logger.info("Desconectado del servidor OPC UA")


if __name__ == "__main__":
    asyncio.run(main())
