import asyncio
import os
import ssl
from datetime import datetime, timedelta, timezone

from aiomqtt import Client as MQTTClient, TLSParameters
from asyncua import Client, ua
from asyncua.crypto import security_policies
from awscrt import auth, http, io, mqtt
from awscrt.exceptions import AwsCrtError
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv

load_dotenv()
import time

## OPCUA
user_ua = os.getenv("UA_USER")
password_ua = os.getenv("UA_PASSWORD")
url_ua = os.getenv("UA_URL")
uri_ua = os.getenv("UA_URI")
cert_ua = os.getenv("UA_CERT")
key_ua = os.getenv("UA_KEY")
server_cert_ua = os.getenv("UA_SERVER_CERT")

## AWS IoT Core
endpoint = os.getenv("AWS_ENDPOINT")
client_id = os.getenv("AWS_CLIENTID")
path_to_certificate = os.getenv("AWS_CERT")
path_to_private_key = os.getenv("AWS_PRIVATE")
path_to_amazon_root_ca_1 = os.getenv("AWS_ROOT")

# Temas para publicar y suscribir
publish_topic = "curva/torque"
subscribe_topic = "curva/current"

# Callback para procesar mensajes entrantes
def on_message_received(topic, payload, **kwargs):
    print(f"Received message from topic '{topic}': {payload}")

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

# Configuración del Proxy
proxy_options = http.HttpProxyOptions(
    host_name='192.168.85.1',  # Cambia 'proxy-hostname' por tu hostname de proxy
    port=3128,  # Cambia 8080 por el puerto de tu proxy
    # Para proxy con autenticación:
    # auth_username='tu-usuario',
    # auth_password='tu-contraseña'
)

async def connect_mqtt(retry_attempts=5, retry_delay=5):
    # Inicialización del loop de eventos para asyncio
    attempt_count = 0
    while attempt_count < retry_attempts:
        try:
            event_loop_group = io.EventLoopGroup(1)
            host_resolver = io.DefaultHostResolver(event_loop_group)
            client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

            mqtt_connection = mqtt_connection_builder.mtls_from_path(
                endpoint=endpoint,
                cert_filepath=path_to_certificate,
                pri_key_filepath=path_to_private_key,
                client_bootstrap=client_bootstrap,
                ca_filepath=path_to_amazon_root_ca_1,
                client_id=client_id,
                clean_session=False,
                keep_alive_secs=6,
                http_proxy_options=None
            )
            connect_future = mqtt_connection.connect()
            await asyncio.wrap_future(connect_future)
            print("Conexión exitosa")
            return mqtt_connection
        except AwsCrtError as e:
            print(f"Falla al conectar al proxy o internet. {e}")
            attempt_count += 1
            await asyncio.sleep(retry_delay)
    raise Exception(f"No se pudo establecer conexión luego de {retry_attempts} intentos")    

class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    def __init__(self):
        pass
        
    async def datachange_notification(self, node, val, data):
        # print("Python: New data change event", node, val)
        key = get_key_by_value(str(node))
        print(f"{key} - {val}")
        publish_to_aws(topic=key, message=str(val))

    def event_notification(self, event):
        print("Python: New event", event)

    
def publish_to_aws(topic, message):
    mqtt_connection.publish(topic=f"prosys/{topic}", payload=message, qos=mqtt.QoS.AT_LEAST_ONCE)
    
async def main():
    ## Configuración de ClientUA
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
    
    global mqtt_connection
    mqtt_connection = await connect_mqtt()    
    
    try:
        await ua_client.connect()
        print("Root children are", await ua_client.nodes.root.get_children())
        var_list = [ua_client.get_node(nodes_dict[i]) for i in nodes_dict.keys()]
        handler = SubHandler()
        sub = await ua_client.create_subscription(1000, handler)
        handle = await sub.subscribe_data_change(var_list)
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await ua_client.disconnect()
        await mqtt_connection.disconnect()
        

# Ejecutar la función principal
if __name__ == '__main__':
    asyncio.run(main())
