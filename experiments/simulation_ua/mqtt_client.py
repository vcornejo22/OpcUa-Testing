import asyncio
import random

from aiomqtt import Client as MQTTClient

broker_mqtt = '172.16.190.1'
port_mqtt = 1883


async def mqtt_publish(key, node, val):
    topic = f"prosys/{key}/{node}"
    print(f"Publish: prosys/{key}/{node} - {val}")
    await mqtt_client.publish(topic, str(val), qos=1)

async def main():
    global mqtt_client 
    mqtt_client = MQTTClient(hostname=broker_mqtt, port=port_mqtt)    
    await mqtt_client.__aenter__()
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
