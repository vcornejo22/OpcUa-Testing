import asyncio
from influxdb_client import Point
from influxdb_client.rest import ApiException
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from asyncua import Client, ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
from aiomqtt import TLSParameters
from aiomqtt import Client as MQTTClient

load_dotenv()
## INFLUX
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url_influx = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")
## MQTT
broker_mqtt = os.getenv("MQTT_BRIDGE_BROKER")
port_mqtt = os.getenv("MQTT_BRIDGE_PORT")
cafile_mqtt=os.getenv("MQTT_BRIDGE_CAFILE")
cert_mqtt = os.getenv("MQTT_BRIDGE_CERT")
key_mqtt = os.getenv("MQTT_BRIDGE_KEY")
tlsv_mqtt = os.getenv("MQTT_BRIDGE_TLSV")
user_mqtt = os.getenv("MQTT_BRIDGE_USER")
password_mqtt = os.getenv("MQTT_BRIDGE_PASSWORD")
## OPCUA
user_ua = os.getenv("UA_USER")
password_ua = os.getenv("UA_PASSWORD")
url_ua = os.getenv("UA_URL")
uri_ua = os.getenv("UA_URI")
cert_ua = os.getenv("UA_CERT")
key_ua = os.getenv("UA_KEY")
server_cert_ua = os.getenv("UA_SERVER_CERT")

nodes_dict = {
    "Constant": "ns=3;i=1001",
    "Counter": "ns=3;i=1002",
    "Random": "ns=3;i=1003",
    "Sawtooth": "ns=3;i=1004",
    "Sinusoid": "ns=3;i=1005",
    "Square": "ns=3;i=1006",
    "Triangle": "ns=3;i=1007",
}


def get_key_by_value(search_value):
    for key, value in nodes_dict.items():
        if value == search_value:
            return key
    return None


async def mqtt_publish(key, node, val):
    topic = f"prosys/{key}/{node}"
    print(f"Publish: prosys/{key}/{node} - {val}")
    await mqtt_client.publish(topic, str(val), qos=1)


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    def __init__(self, write_api, bucket, org, timeout=60):
        self.last_val = {}
        self.timer_task = {}
        self.write_api = write_api
        self.bucket = bucket
        self.org = org
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
                print(f"Insert Timeout: {key}")
                last_val = self.last_val[key]
                point = Point("UA").tag("Prosys", key).field(node, last_val)
                await self.write_api.write(
                    bucket=self.bucket, org=self.org, record=point
                )

    async def datachange_notification(self, node, val, data):
        # print("Python: New data change event", node, val)
        key = get_key_by_value(str(node))
        val = float(val)
        if key in self.last_val and val != self.last_val[key] and key == "Constant":
            last_val = self.last_val[key]
            
            point = (
                Point("UA")
                .tag("Prosys", key)
                .field(node, last_val)
                .time(datetime.now(timezone.utc) - timedelta(seconds=1))
            )
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"Last value: {self.last_val}")

        self.last_val[key] = val
        await self.reset_timer(key, node)

        point = Point("UA").tag("Prosys", key).field(node, val)
        await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        await mqtt_publish(key, node, val)

    def event_notification(self, event):
        print("Python: New event", event)


async def main():
    ua_client = Client(url=url_ua, timeout=60 * 10)
    ua_client.set_user(user_ua)
    ua_client.set_password(password_ua)
    await ua_client.set_security(
        security_policies.SecurityPolicyBasic256Sha256,
        certificate=cert_ua,
        private_key=key_ua,
        mode=ua.MessageSecurityMode.SignAndEncrypt
    )
    ua_client.application_uri = uri_ua
    global mqtt_client
    tls_parameters = TLSParameters(ca_certs=cafile_mqtt, certfile=cert_mqtt, keyfile=key_mqtt)#, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client = MQTTClient(hostname=broker_mqtt, port=int(port_mqtt), username=user_mqtt, password=password_mqtt, tls_params=tls_parameters, tls_insecure=False)
    await mqtt_client.__aenter__()

    async with InfluxDBClientAsync(url=url_influx, token=token, org=org) as influx_client:
        write_api = influx_client.write_api()
        await ua_client.connect()
        # await client.load_data_type_definitions(overwrite_existing=True)
        print("Root children are", await ua_client.nodes.root.get_children())
        var_list = [ua_client.get_node(nodes_dict[i]) for i in nodes_dict.keys()]
        handler = SubHandler(write_api, bucket, org)
        sub = await ua_client.create_subscription(1000, handler)
        handle = await sub.subscribe_data_change(var_list)
        while True:
            await asyncio.sleep(1)

## Comannd to subscribe to broker
# mosquitto_sub -h Victors-MacBook-Pro.local -p 8889 --cafile mqtt-bridge/conf/certs/ca-mac-bridge.crt --cert mqtt-bridge/conf/certs/server-mac-bridge.crt --key mqtt-bridge/conf/certs/server-mac-bridge.key --tls-version tlsv1.2 -t prosys/# -u bugny -P 131492 -d

if __name__ == "__main__":
    asyncio.run(main())
