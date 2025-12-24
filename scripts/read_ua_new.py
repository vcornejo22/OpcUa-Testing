import asyncio
import os
from datetime import datetime, timedelta, timezone

from aiomqtt import Client as MQTTClient, TLSParameters
from asyncua import Client, ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.rest import ApiException

load_dotenv()

nodes_dict = {
            "Constant": "ns=3;i=1001",
            "Counter": "ns=3;i=1002",
            "Random": "ns=3;i=1003",
            "Sawtooth": "ns=3;i=1004",
            "Sinusoid": "ns=3;i=1005",
            "Square": "ns=3;i=1006",
            "Triangle": "ns=3;i=1007",
        }

class Config:
    """
    Singleton class to load environment configuration
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.load_config()
        return cls._instance

    def load_config(self):
        self.influx_token = os.getenv("INFLUXDB_TOKEN")
        self.influx_org = os.getenv("INFLUXDB_ORG")
        self.influx_url = os.getenv("INFLUXDB_URL")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET")

        self.mqtt_broker = os.getenv("MQTT_BROKER")
        self.mqtt_port = int(os.getenv("MQTT_PORT"))
        self.mqtt_cafile = os.getenv("MQTT_CAFILE")
        self.mqtt_cert = os.getenv("MQTT_CERT")
        self.mqtt_key = os.getenv("MQTT_KEY")
        self.mqtt_user = os.getenv("MQTT_USER")
        self.mqtt_password = os.getenv("MQTT_PASSWORD")

        self.ua_user = os.getenv("UA_USER")
        self.ua_password = os.getenv("UA_PASSWORD")
        self.ua_url = os.getenv("UA_URL")
        self.ua_uri = os.getenv("UA_URI")
        self.ua_cert = os.getenv("UA_CERT")
        self.ua_key = os.getenv("UA_KEY")
        self.ua_server_cert = os.getenv("UA_SERVER_CERT")

class MQTTHandler:
    """
    Handles MQTT connections and publishing
    """
    def __init__(self, config):
        self.config = config
        self.client = None

    async def connect(self):
        tls_params = TLSParameters(
            ca_certs=self.config.mqtt_cafile,
            certfile=self.config.mqtt_cert,
            keyfile=self.config.mqtt_key
        )
        self.client = MQTTClient(
            hostname=self.config.mqtt_broker,
            port=self.config.mqtt_port,
            username=self.config.mqtt_user,
            password=self.config.mqtt_password,
            tls_params=tls_params,
            tls_insecure=False
        )
        await self.client.__aenter__()

    async def publish(self, topic, payload):
        await self.client.publish(topic, str(payload), qos=1)

class InfluxDBHandler:
    """
    Handles InfluxDB connections and data writing
    """
    def __init__(self, config):
        self.config = config
        self.client = None
        self.write_api = None

    async def connect(self):
        self.client = InfluxDBClientAsync(
            url=self.config.influx_url,
            token=self.config.influx_token,
            org=self.config.influx_org
        )
        self.write_api = self.client.write_api()

    async def write_point(self, point):
        await self.write_api.write(
            bucket=self.config.influx_bucket,
            org=self.config.influx_org,
            record=point
        )

class OPCUAHandler:
    """
    Handles OPC UA client connection and subscription
    """
    def __init__(self, config, influx_handler, mqtt_handler, nodes_dict):
        self.config = config
        self.client = None
        self.influx_handler = influx_handler
        self.mqtt_handler = mqtt_handler
        self.nodes_dict = nodes_dict

    async def connect(self):
        self.client = Client(url=self.config.ua_url, timeout=60 * 10)
        self.client.set_user(self.config.ua_user)
        self.client.set_password(self.config.ua_password)
        await self.client.set_security(
            security_policies.SecurityPolicyBasic256Sha256,
            certificate=self.config.ua_cert,
            private_key=self.config.ua_key,
            mode=ua.MessageSecurityMode.SignAndEncrypt
        )
        self.client.application_uri = self.config.ua_uri
        await self.client.connect()

    async def subscribe(self):
        handler = SubscriptionHandler(self.influx_handler, self.mqtt_handler, self.nodes_dict)
        subscription = await self.client.create_subscription(1000, handler)
        nodes = [self.client.get_node(node) for node in self.nodes_dict.values()]
        await subscription.subscribe_data_change(nodes)

class SubscriptionHandler:
    """
    Handles data change notifications from OPC UA server
    """
    def __init__(self, influx_handler, mqtt_handler, nodes_dict, timeout=60):
        self.influx_handler = influx_handler
        self.mqtt_handler = mqtt_handler
        self.nodes_dict = nodes_dict
        self.last_val = {}
        self.timer_task = {}
        self.timeout = timeout
        self.value_changed = False

    async def reset_timer(self, key, node):
        self.value_changed = True
        if key in self.timer_task:
            self.timer_task[key].cancel()
        self.value_changed = False
        self.timer_task[key] = asyncio.create_task(
            self.insert_last_value_after_timeout(key, node)
        )

    async def insert_last_value_after_timeout(self, key, node):
        while not self.value_changed:
            await asyncio.sleep(self.timeout)
            if key in self.last_val:
                last_val = self.last_val[key]
                point = Point("UA").tag("Prosys", key).field(node, last_val)
                await self.influx_handler.write_point(point)

    async def datachange_notification(self, node, val, data):
        key = self.get_key_by_value(str(node))
        val = float(val)

        if key in self.last_val and val != self.last_val[key] and key == "Constant":
            last_val = self.last_val[key]
            point = (
                Point("UA")
                .tag("Prosys", key)
                .field(node, last_val)
                .time(datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            await self.influx_handler.write_point(point)

        self.last_val[key] = val
        await self.reset_timer(key, node)
        point = Point("UA").tag("Prosys", key).field(node, val)
        await self.influx_handler.write_point(point)
        await self.mqtt_handler.publish(f"prosys/{key}/{node}", val)

    def get_key_by_value(self, search_value):
        for key, value in self.nodes_dict.items():
            if value == search_value:
                return key
        return None

async def main():
    config = Config()
    influx_handler = InfluxDBHandler(config)
    mqtt_handler = MQTTHandler(config)
    opc_ua_handler = OPCUAHandler(config, influx_handler, mqtt_handler, nodes_dict)

    await influx_handler.connect()
    await mqtt_handler.connect()
    await opc_ua_handler.connect()
    await opc_ua_handler.subscribe()

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())