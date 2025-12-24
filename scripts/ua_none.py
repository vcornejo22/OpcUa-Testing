import asyncio
import os
from typing import Union

from asyncua import Client
from asyncua import Node
from asyncua.ua.uaprotocol_auto import DataChangeNotification
from dotenv import load_dotenv

def require_env(value: str | None, name: str) -> str:
    if value is None:
        raise ValueError(f"Environment variable {name} is required but not set.")
    return value

load_dotenv(override=True)
# OPC UA Configuración
UA_URL=require_env(os.getenv("UA_URL"), "UA_URL")
UA_URI=require_env(os.getenv("UA_URI"), "UA_URI")
# UA_URL='opc.tcp://mac:53530/OPCUA/SimulationServer'
# UA_URI='urn:mac:OPCUA:SimulationServer'
UA_USER='user'
UA_PASSWORD='user'

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
def get_key_by_value(search_value: str):
    return next((key for key, value in nodes_dict.items() if value == search_value), None)

# Clase manejadora de suscripción OPC UA
class SubHandler:

    async def datachange_notification(self, node: Node, val: Union[int, float], data: DataChangeNotification):
        key = get_key_by_value(str(node))
        print(f"Python: key: {key} - Nodo: {node} - val: {val}")

    def event_notification(self, event: str):
        print("Python: New event", event)

# Función principal
async def main():
    ua_client = Client(url=UA_URL)
    ua_client.set_user(UA_USER)
    ua_client.set_password(UA_PASSWORD)
    ua_client.application_uri = UA_URI
            
    try: 
        await ua_client.connect()
        print(f"Conectado al servidor OPC UA: {UA_URL}")
        
        # Crear lista de nodos a suscribirse
        var_list = [ua_client.get_node(node_id) for node_id in nodes_dict.values()]
        handler = SubHandler()
        sub = await ua_client.create_subscription(1000, handler)
        # sub: Subscription = cast(Subscription, await ua_client.create_subscription(1000, handler))
        await sub.subscribe_data_change(var_list)

        while True:
            await asyncio.sleep(1)            
                    
    except KeyboardInterrupt:
        print("Finalizando aplicación por interrupción de teclado")
        
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        
    finally:
        await ua_client.disconnect()
        print("Desconectado del servidor OPC UA")

if __name__ == "__main__":
    asyncio.run(main())