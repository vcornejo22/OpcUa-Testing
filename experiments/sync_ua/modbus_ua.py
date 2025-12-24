import json
import logging
import os
import random
import socket
import struct
import threading
import time
from typing import Tuple

import paho.mqtt.client as mqtt
from asyncua import ua
from asyncua.crypto import security_policies
from asyncua.sync import Client
from dotenv import load_dotenv

load_dotenv()
UA_URL = "opc.tcp://ascs001:44683/ABB/800xA/OPC/UA/Server"
UA_URI = os.getenv("UA_URI")
UA_USER = os.getenv("UA_USER")
UA_PASSWORD = os.getenv("UA_PASSWORD")
UA_CERT = os.getenv("UA_CERT")
UA_KEY = os.getenv("UA_KEY")

# Configuracion logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='modbus_mqtt.log')
logger = logging.getLogger(__name__)

device_sku = [
    # {'sku': 'FSSRVRT-LI-ZD100002', 'address': 2},
    # {'sku': 'FSSRVRT-LI-ZD100003', 'address': 3},
    # {'sku': 'FSSRVRT-LI-ZD100004', 'address': 4},
    # {'sku': 'FSSRVRT-LI-ZD100005', 'address': 5},
    # {'sku': 'FSSRVRT-LI-ZD100006', 'address': 6},
    # {'sku': 'FSSRVRT-LI-ZD100007', 'address': 7},
    # {'sku': 'FSSRVRT-LI-ZD100008', 'address': 8},
    # {'sku': 'FSSRVRT-LI-ZD100009', 'address': 9},
    # {'sku': 'FSSRVRT-LI-ZD100011', 'address': 11},
    # {'sku': 'FSSRVRT-LI-ZD100013', 'address': 13},
    # {'sku': 'FSSRVRT-LI-ZD100014', 'address': 14},
    # {'sku': 'FSSRVRT-LI-ZD100015', 'address': 15},
    # {'sku': 'FSSRVRT-LI-ZD100018', 'address': 18},
    # {'sku': 'FSSRVRT-LI-ZD100019', 'address': 19},
    # {'sku': 'FSSRVRT-LI-ZD100020', 'address': 20},
    # {'sku': 'FSSRVRT-LI-ZD100021', 'address': 21},
    # {'sku': 'FSSRVRT-LI-ZD100022', 'address': 22},
    # {'sku': 'FSSRVRT-LI-ZD100023', 'address': 23},
    # {'sku': 'FSSRVRT-LI-ZD100024', 'address': 24},
    {'sku': 'FSSRVRT-LI-ZD100025', 'address': 25},
    # {'sku': 'FSSRVRT-LI-ZD100026', 'address': 26},
    # {'sku': 'FSSRVRT-LI-ZD100027', 'address': 27},
    # {'sku': 'FSSRVRT-LI-ZD100028', 'address': 28},
]

## OPC UA
value1 = "ns=2;b=LkFl/DVwY0SkMcd7k+olrKrgFtNrxaxLt2+BlUo18bh2YWx1ZTE="
value2 = "ns=2;b=LkFl/DVwY0SkMcd7k+olrKrgFtNrxaxLt2+BlUo18bh2YWx1ZTI="

class SubHandler:
    def datachange_notification(self, node, val, data):
        logger.info(f"Data: {node} - {val}")
        
    def event_notification(self, event):
        logger.info(f"Event: {event}")

class UCClient:
    def __init__(self, url: str, timeout: int) -> None:
        self.url = url
        self.timeout = timeout * 10
    
    def conf_security(self, username: str, password: str, cert, key, uri, server_cert):
        self.client.set_user
        
        
        
class ModbusDevice:
    def __init__(self, sku: str, address: int):
        self.sku = sku
        self.address = address

    @staticmethod
    def calculate_crc(data: bytes) -> int:
        """
        Cálculo de sumas de comprobación CRC-16/MODBUS
        Polinomio: x16 + x15 + x2 + 1
        """
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x0001:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc

    def create_request_message(self) -> bytes:
        """
        Crear mensaje de solicitud con suma de comprobación CRC
        """
        message = bytes([self.address, 0x03, 0x00, 0x01, 0x00, 0x05])
        crc = ModbusDevice.calculate_crc(message)
        message += struct.pack("<H", crc)
        return message

    @staticmethod
    def parse_response(response: bytes) -> Tuple[float, float, int, float]:
        """
        Análisis de datos de respuesta
        Devuelve: (Aceleración, Velocidad, Desplazamiento, Temperatura)
        """
        if len(response) < 15:
            raise ValueError("Longitud insuficiente de los datos de respuesta")

        acceleration = struct.unpack(">H", response[3:5])[0] / 10.0
        velocity = struct.unpack(">H", response[5:7])[0] / 10.0
        displacement = struct.unpack(">H", response[7:9])[0]
        temperature = struct.unpack(">f", response[9:13])[0]

        return acceleration, velocity, displacement, temperature

class MQTTClient:
    def __init__(self, broker_address: str, broker_port: int, username: str = None, password: str = None):
        self.client_id = f"publish-{random.randint(0,100)}"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.unacked_publish = set()
        self.client.user_data_set(self.unacked_publish)
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.username = username
        self.password = password

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Conectado al broker MQTT en {self.broker_address}:{self.broker_port}")
        else:
            logger.error(f"Error al conectar al broker MQTT, codigo de retorno {rc}")
        
    def connect(self):
        try: 
            self.client.on_connect = self.on_connect
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
            self.client.connect(self.broker_address, self.broker_port)
            # self.client.loop_start()
            # logger.info(f"[+] Conectado al broker MQTT en {self.broker_address}:{self.broker_port}")
        except Exception as e:
            logger.error(f"[!] Error al conectar al broker MQTT: {e}")
            
    def publish(self, topic:str, message: dict):
        try: 
            message_json = json.dumps(message)
            self.client.publish(topic, message_json)
            logger.info(f"[+] Publicacion exitosa en el topico '{topic}'")
        except Exception as e:
            logger.error(f"[!] Error al publicar al broker MQTT: {str(e)}")

class OpcUAClient:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(OpcUAClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, url: str, user:str = None, password: str = None, uri: str = None, cert: str = None, key: str = None):    
        if not hasattr(self, "initialized"): # Evita volver a inicializar en multiples instancias
            self.url = url
            self.user = user
            self.password = password 
            self.uri = uri
            self.cert = cert
            self.key = key
            self.client = None
            self.initialized = True
            
    def connect(self):
        try:
            # Inicializar el cliente
            self.client = Client(url=self.url)
            if self.user and self.password:    
                self.client.set_user(self.user)
                self.client.set_password(self.password)
                self.client.set_security(
                    security_policies.SecurityPolicyBasic256Sha256,
                    certificate=self.cert,
                    private_key=self.key,
                    mode=ua.MessageSecurityMode.SignAndEncrypt
                )
            self.client.application_uri = self.uri
            self.client.connect()
            logger.info("[+] Conectado al servidor OPC UA")
        except Exception as e:
            logger.error(f"[!] Error al conectar al servidor OPC UA: {e}")
            
    def disconnect(self):
        if self.client:
            self.client.disconnect()
            logger.info("[+] Desconectado del servidor OPC UA")
        
class ModbusClient:
    def __init__(self, host: str, port: int, retry_interval: int = 1, max_retries: int =3):
        self.host = host
        self.port = port
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.devices = [ModbusDevice(**device) for device in device_sku]
        
    def receive_with_timeout(self, sock: socket.socket, expected_length: int = 15, timeout: float = 2.0) -> bytes:
        """
        Funciones de recepción de datos con tiempo de espera
        """
        sock.settimeout(timeout)
        start_time = time.time()
        data = b""

        try:
            while len(data) < expected_length:
                if time.time() - start_time > timeout:
                    raise socket.timeout("Tiempo de espera de recepción")

                chunk = sock.recv(expected_length - len(data))
                if not chunk:
                    raise ConnectionError("Conexión cerrada")
                data += chunk

            return data
        except socket.timeout:
            raise socket.timeout(f"Tiempo de espera de recepción: Recibido {len(data)} byte, esperar {expected_length} bocados")
        
    def read_loop(self):
        while True:
            self.run()
            time.sleep(1)
    
    def run(self):
        # Publicar los datos a dos brokers MQTT
        mqtt_client_1 = MQTTClient(broker_address="127.0.0.1", broker_port=1883)
        mqtt_client_2 = MQTTClient(broker_address="127.0.0.1", broker_port=1884, username="bugny", password="131492")
        mqtt_client_1.connect()
        mqtt_client_2.connect()
        
        # Crear instancia de OpcUaClient
        ua_client = OpcUAClient(
            url=UA_URL,
            user=UA_USER,
            password=UA_PASSWORD,
            uri=UA_URI,
            cert=UA_CERT,
            key=UA_KEY
        )
        ua_client.connect()
        var = ua_client.client.get_node(value1)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)
                s.connect((self.host, self.port))
                logger.info(f"[+] Conectado al servidor Modbus: {self.host}:{self.port}")
                while True: 
                        
                    for device in self.devices:
                        retries = 0

                        while retries < self.max_retries:
                            try:
                                # Enviar solicitud
                                request = device.create_request_message()
                                s.send(request)
                                logger.debug(f"\nEnviar una solicitud al dispositivo {device.sku} (dirección: {device.address}): {request.hex(' ')}")

                                # Recibir respuesta
                                response = self.receive_with_timeout(s)
                                logger.debug(f"Respuesta recibida: {response.hex(' ')}")

                                # Analizar los datos de respuesta
                                acceleration, velocity, displacement, temperature = ModbusDevice.parse_response(response)
                                
                                topic = f"devices/{device.sku}/measurements"
                                message = {
                                    "device_id": device.sku,
                                    "timestamp": int(time.time()),  # Marca de tiempo en formato Unix
                                    "measurements": {
                                        "acceleration": {"value": acceleration, "unit": "m/s²"},
                                        "velocity": {"value": velocity, "unit": "mm/s"},
                                        "displacement": {"value": displacement, "unit": "µm"},
                                        "temperature": {"value": temperature, "unit": "°C"},
                                    },
                                }
                                if device.sku == "FSSRVRT-LI-ZD100025":
                                    var.write_value(round(temperature,5))
                                    print(temperature)
                                # Publicar en ambos brokers
                                mqtt_client_1.publish(topic, message)
                                mqtt_client_2.publish(topic, message)

                                # Esperar un breve periodo antes de la siguiente solicitud
                                time.sleep(1)
                                break  # Salir del bucle de reintento en caso de éxito

                            except (socket.timeout, ConnectionError) as e:
                                retries += 1
                                logger.warning(f"Dispositivo {device.sku} error de comunicación (intento {retries}/{self.max_retries}): {str(e)}")
                                if retries < self.max_retries:
                                    logger.warning(f"Esperar {self.retry_interval} segundos antes de reintentar...")
                                    time.sleep(self.retry_interval)
                                else:
                                    logger.error(f"Dispositivo {device.sku} comunicación fallida, continuar con el siguiente dispositivo")

                            except Exception as e:
                                logger.error(f"Dispositivo {device.sku} se produjo un error: {str(e)}")
                                break  # Otros errores omiten directamente el dispositivo actual

        except ConnectionRefusedError:
            logger.error("Conexión denegada, compruebe si la dirección del servidor y el puerto son correctos.")
        except Exception as e:
            logger.error(f"Se produjo un error: {str(e)}")


        
def main():
    client = ModbusClient("127.0.0.1", port=8234)
    read_thread = threading.Thread(target=client.read_loop, daemon=True)
    read_thread.start()
    
    # Mantener el programa en ejecucion
    try:
        while True: 
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Programa terminado por el usuario")
 
if __name__ == "__main__":
    main()