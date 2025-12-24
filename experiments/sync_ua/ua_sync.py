import logging
import os
import time

from asyncua import ua
from asyncua.crypto import security_policies
from asyncua.sync import Client
from dotenv import load_dotenv

load_dotenv()


UA_URL = os.getenv("UA_URL")
UA_USER = os.getenv("UA_USER")
UA_PASSWORD = os.getenv("UA_PASSWORD")
UA_URI = os.getenv("UA_URI")
UA_CERT = os.getenv("UA_CERT")
UA_KEY = os.getenv("UA_KEY")

if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    # client = Client("opc.tcp://macbookpro:53530/OPCUA/SimulationServer")
    client = Client(UA_URL)
    # client.set_security(
    #         security_policies.SecurityPolicyBasic256Sha256,
    #     certificate=UA_CERT,
    #     private_key=UA_KEY,
    #     mode=ua.MessageSecurityMode.SignAndEncrypt
    #     )
    # client.set_user(UA_USER)
    # client.set_password(UA_PASSWORD)
    # client.application_uri = "urn:Victors-MacBook-Pro.local:OPCUA:SimulationServer"
    client.application_uri = UA_URI
        
    client.connect()    
    root = client.nodes.root
    print("Root is", root)
    
    # var = client.get_node("ns=3;i=1001")
    
    # while True: 
    #     a = var.read_value()
    #     print(a)
    #     time.sleep(1)