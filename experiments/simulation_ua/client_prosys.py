import asyncio
from asyncua import Client, ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv
import os
from aiomqtt import Client as MQTTClient
from aiomqtt import TLSParameters
import asyncua 
import logging 

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("OPCUA_MQTT_Client")

# Desactivar o reducir los logs de la librería asyncua
logging.getLogger("asyncua").setLevel(logging.WARNING)

load_dotenv()
UA_URL = "opc.tcp://Victors-MacBook-Pro.local:53530/OPCUA/SimulationServer"
UA_URI = "urn:Victors-MacBook-Pro.local:OPCUA:SimulationServer"
UA_USER = "bugny"
UA_PASSWORD = "131492"
UA_CERT = "../opcua/certs/cert-prosys.der"
UA_KEY = "../opcua/certs/key-prosys.pem"

## MQTT
broker_mqtt = '172.16.190.1'
port_mqtt = 8883
user_mqtt = 'bugny'
password_mqtt = '131492'
cert_mqtt = "./conf/certs/server-mac.crt"
key_mqtt = "./conf/certs/server-mac.key"
cafile_mqtt = "./conf/certs/ca-mac.crt"

number_nodes = 1607
nodes = [f"ns=3;i={i}" for i in range(1001,number_nodes) ]

async def mqtt_publish(node, val):
    topic = f"prosys/{node}"
    # logger.info(f"Publish: prosys/{node}/ {val}")
    await mqtt_client.publish(topic, str(val), qos=1)


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    async def datachange_notification(self, node, val, data):
        # logger.info("Python: New data change event", node, val)
        await mqtt_publish(node, val)

    def event_notification(self, event):
        logger.info("Python: New event", event)


async def main():
    # MQTT
    global mqtt_client
    tls_parameters = TLSParameters(ca_certs=cafile_mqtt, certfile=cert_mqtt, keyfile=key_mqtt)#, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client = MQTTClient(hostname=broker_mqtt, port=int(port_mqtt), username=user_mqtt, password=password_mqtt, tls_params=tls_parameters, tls_insecure=False)
    await mqtt_client.__aenter__()
    mqtt_client.pending_calls_threshold = 2000 # Número de callbacks
    logger.info(f"Connected to MQTT Broker at {broker_mqtt}:{port_mqtt}")
    
    # OPC UA
    ua_client = Client(url=UA_URL)
    ua_client.set_user(UA_USER)
    ua_client.set_password(UA_PASSWORD)
    await ua_client.set_security(
        security_policies.SecurityPolicyBasic256Sha256,
        certificate=UA_CERT,
        private_key=UA_KEY,
        mode=ua.MessageSecurityMode.SignAndEncrypt
    )
    ua_client.application_uri = UA_URI
    while True:
        try:
            await ua_client.connect()
            logger.info(f"Connected to OPC UA server at {UA_URL}")
            root_children = await ua_client.nodes.root.get_children()
            logger.debug("Root children are", root_children)
            var_list = [ua_client.get_node(i) for i in nodes]
            handler = SubHandler()
            sub = await ua_client.create_subscription(500, handler)
            handle = await sub.subscribe_data_change(var_list)
            logger.info(f"Subscribed to {len(var_list)} OPC UA nodes")
            
            while True:
                await asyncio.sleep(0.5)
                
        except KeyboardInterrupt:
            logger.error("Received KeyboardInterrupt, shutting down gracefully...")
            await sub.unsubscribe(handler)
            break
        
        except asyncua.ua.uaerrors._base.UaError as e:
            logger.error(f"OPC UA Error: {e}")   
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            
        finally:
            logger.info("Disconnecting from OPC UA Server")
            await ua_client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Criticla eror in main: {e}", exc_info=True)