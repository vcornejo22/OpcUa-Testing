import asyncio
import os

from aiomqtt import Client as MQTTClient, TLSParameters
from awscrt import http, io, mqtt
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv

load_dotenv()
# mosquitto_sub -h mqtt-02 -p 8883 --cafile ca-mqtt-02.crt --cert server-mqtt-02.crt --key server-mqtt-02.key --tls-version tlsv1.2 -t server -u soporte -P 5oporte.SHP2023 -d
## MQTT Raspberry
raspi_endpoint = "mqtt-02"
raspi_port = "8883"
raspi_certificate = "certs_external/server-mqtt-02.crt"
raspi_private_key = "certs_external/server-mqtt-02.key"
raspi_root = "certs_external/ca-mqtt-02.crt"
raspi_user = "soporte"
raspi_password = "5oporte.SHP2023"

## AWS IoT Core
endpoint = os.getenv("AWS_ENDPOINT")
client_id = os.getenv("AWS_CLIENTID")
path_to_certificate = os.getenv("AWS_CERT")
path_to_private_key = os.getenv("AWS_PRIVATE")
path_to_amazon_root_ca_1 = os.getenv("AWS_ROOT")

# Temas para publicar y suscribir
subscribe_topic = "server"

# Configuración del Proxy
proxy_options = http.HttpProxyOptions(
    host_name='192.168.85.1',  # Cambia 'proxy-hostname' por tu hostname de proxy
    port=3128,  # Cambia 8080 por el puerto de tu proxy
    # Para proxy con autenticación:
    # auth_username='tu-usuario',
    # auth_password='tu-contraseña'
)

async def connect_to_aws():
    # Inicialización del loop de eventos para asyncio
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
        http_proxy_options=proxy_options
    )
    connect_future = mqtt_connection.connect()
    await asyncio.wrap_future(connect_future)
    return mqtt_connection
    

async def main():
    ## MQTT Local
    tls_parameters = TLSParameters(ca_certs=raspi_root, certfile=raspi_certificate, keyfile=raspi_private_key)#, tls_version=ssl.PROTOCOL_TLSv1_2)
    mqtt_client = MQTTClient(hostname=raspi_endpoint, port=int(raspi_port), username=raspi_user, password=raspi_password, tls_params=tls_parameters, tls_insecure=False)
    await mqtt_client.__aenter__()
    
    ## MQTT AWS
    global mqtt_aws
    mqtt_aws = await connect_to_aws()
    # try:
    await mqtt_client.subscribe(subscribe_topic)
    async for message in mqtt_client.messages:
        print(f"Topic: {message.topic.value} - Value: {message.payload}")
        mqtt_aws.publish(topic=message.topic.value, payload=message.payload, qos=mqtt.QoS.AT_LEAST_ONCE)
    # except Error as e:
    #     pass

        

# Ejecutar la función principal
if __name__ == '__main__':
    asyncio.run(main())
