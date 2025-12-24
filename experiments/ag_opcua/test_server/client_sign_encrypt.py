import asyncio

from asyncua import Client, ua
from asyncua.crypto import security_policies

UA_URL="opc.tcp://Victors-MacBook-Pro.local:53530/OPCUA/SimulationServer"
UA_URI="urn:Victors-MacBook-Pro.local:OPCUA:SimulationServer"
UA_USER='bugny'
UA_PASSWORD='131492'
# UA_CERT="./opcua/certs/cert-prosys.der"
# UA_KEY="./opcua/certs/key-prosys.pem"

nodes_dict = {
    "Constant": "ns=3;i=1001",
    "Counter": "ns=3;i=1002",
    "Random": "ns=3;i=1003",
    "Sawtooth": "ns=3;i=1004",
    "Sinusoid": "ns=3;i=1005",
    "Square": "ns=3;i=1006",
    "Triangle": "ns=3;i=1007",
}

## Aes256Sha256RsaPss
UA_CERT="./opcua/Aes256Sha256RsaPss/certificate.pem"
UA_KEY="./opcua/Aes256Sha256RsaPss/key.pem"

def get_key_by_value(search_value):
    for key, value in nodes_dict.items():
        if value == search_value:
            return key
    return None

class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    async def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)
        # key = get_key_by_value(str(node))

    def event_notification(self, event):
        print("Python: New event", event)


async def main():
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

if __name__ == "__main__":
    asyncio.run(main())