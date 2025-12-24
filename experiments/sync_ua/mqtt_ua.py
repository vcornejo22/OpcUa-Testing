from asyncua.sync import Client
from asyncua import ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv
import os 
import time

load_dotenv()
## OPCUA
user_ua = os.getenv("UA_USER")
password_ua = os.getenv("UA_PASSWORD")
url_ua = os.getenv("UA_URL")
uri_ua = os.getenv("UA_URI")
cert_ua = os.getenv("UA_CERT")
key_ua = os.getenv("UA_KEY")
server_cert_ua = os.getenv("UA_SERVER_CERT")

# Broker MQTT 
broker_mqtt = ""
port_mqtt = ""
user_mqtt = ""
password_mqtt = ""
tls_parameters = ""

def main():
    ua_client = Client(url=url_ua)
    ua_client.set_user(user_ua)
    ua_client.set_password(password_ua)
    ua_client.set_security(
        security_policies.SecurityPolicyBasic256Sha256,
        certificate="cert-prosys.der",
        private_key="key-prosys.pem",
        mode=ua.MessageSecurityMode.SignAndEncrypt
    )
    ua_client.application_uri = uri_ua
    
    ua_client.connect()
    
    var = ua_client.get_node("ns=3;i=1001")
    while True:
        a = var.read_value()
        print(a)
        a += 0.1
        var.write_value(a)
        time.sleep(1)
        
        
if __name__ == "__main__":
    main()
        