import socket
import struct
import time
import json
from typing import Tuple
# import requests
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util import Retry


# requests_session = requests.Session()
# # 设置 post()方法进行重访问
# requests_session.mount('http://', HTTPAdapter(max_retries=Retry(total=3, allowed_methods=frozenset(['GET', 'POST']))))

# url_base ='http://127.0.0.1:8080'

device_sku = [
    {'sku': 'FSSRVRT-LI-ZD100002', 'addr':  0x02},
    {'sku': 'FSSRVRT-LI-ZD100003', 'addr':  0x03},
    {'sku': 'FSSRVRT-LI-ZD100004', 'addr':  0x04},
    {'sku': 'FSSRVRT-LI-ZD100005', 'addr':  0x05},
    {'sku': 'FSSRVRT-LI-ZD100006', 'addr':  0x06},
    {'sku': 'FSSRVRT-LI-ZD100007', 'addr':  0x07},
    {'sku': 'FSSRVRT-LI-ZD100008', 'addr':  0x08},
    {'sku': 'FSSRVRT-LI-ZD100009', 'addr':  0x09},
    {'sku': 'FSSRVRT-LI-ZD100011', 'addr':  0x0B},
    {'sku': 'FSSRVRT-LI-ZD100013', 'addr':  0x0D},
    {'sku': 'FSSRVRT-LI-ZD100014', 'addr':  0x0E},
    {'sku': 'FSSRVRT-LI-ZD100015', 'addr':  0x0F},
    {'sku': 'FSSRVRT-LI-ZD100018', 'addr':  0x12},
    {'sku': 'FSSRVRT-LI-ZD100019', 'addr':  0x13},
    {'sku': 'FSSRVRT-LI-ZD100020', 'addr':  0x14},
    {'sku': 'FSSRVRT-LI-ZD100021', 'addr':  0x15},
    {'sku': 'FSSRVRT-LI-ZD100022', 'addr':  0x16},
    {'sku': 'FSSRVRT-LI-ZD100026', 'addr':  0x1A},
    {'sku': 'FSSRVRT-LI-ZD100027', 'addr':  0x1B},
    {'sku': 'FSSRVRT-LI-ZD100028', 'addr':  0x1C},
]

# def post_record(result_json=[], sku=''):
#     server_url = url_base + "/segment-sensor/update-batch-analysis"
#     headers = {'Content-Type': 'application/json'}
#     datas = json.dumps({"resultJson": result_json, "sku": sku})
#     r = requests_session.post(server_url, data=datas, headers=headers, timeout=10)
#     msg = "fail"
#     print(f"{datas}")
#     if r.status_code == requests.codes.ok:
#         dict_r = r.json()
#         print(f"{str(dict_r)}")
#         # msg = dict_r["message"]
#         msg = 'okk'
#     else:
#         msg = 'not okk'
#     print(f"{sku}=====>{msg}")


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


def create_request_message(address) -> bytes:
    """
    Crear mensaje de solicitud con suma de comprobación CRC
    """
    message = bytes([address, 0x03, 0x00, 0x01, 0x00, 0x05])
    crc = calculate_crc(message)
    message += struct.pack("<H", crc)
    return message


def parse_response_val(response: bytes) -> Tuple[float, float, int, float]:
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


def check_response_crc(response: bytes) -> bool:
    print(f"{response[:-2]}")
    crc_calc = calculate_crc(response[:-2])
    crc_calc = struct.pack("<H", crc_calc)
    response_calc = response[:-2] + crc_calc
    print(f"Datos devueltos:{response.hex()} ==== El valor de la suma de comprobación de datos devuelta:{response_calc.hex()}")
    return response.hex() == response_calc.hex()


def parse_response(response: bytes, addr) -> Tuple[int, int, int, int, int, int, int, int]:
    """
    Análisis de datos de respuesta
    Devuelve: (Aceleración, Velocidad, Desplazamiento, Temperatura)
    """
    if len(response) < 15:
        raise ValueError("Longitud insuficiente de los datos de respuesta")
    hex_str = response.hex()
    value1 = int(hex_str[0:1], 16)
    value2 = int(hex_str[1:2], 16)
    value3 = int(hex_str[2:3], 16)
    value4 = int(hex_str[3:5], 16)
    value5 = int(hex_str[5:7], 16)
    value6 = int(hex_str[7:9], 16)
    value7 = int(hex_str[9:11], 16)
    value8 = int(hex_str[11:13], 16)

    # value1 = struct.unpack(">B", response[0:1])[0]
    # value2 = struct.unpack(">B", response[1:2])[0]
    # value3 = struct.unpack(">B", response[2:3])[0]
    # value4 = struct.unpack(">H", response[3:5])[0]
    # value5 = struct.unpack(">H", response[5:7])[0]
    # value6 = struct.unpack(">H", response[7:9])[0]
    value7 = struct.unpack(">H", response[9:11])[0]
    value8 = struct.unpack(">H", response[11:13])[0]
    return value1, value2, value3, value4, value5, value6, value7, value8


def receive_with_timeout(sock: socket.socket, expected_length: int = 15, timeout: float = 2.0) -> bytes:
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
        raise socket.timeout(f"Tiempo de espera de recepción: Aceptado {len(data)} byte, esperar {expected_length} bocados")


def modbus_client():
    """
    Función principal: establecer conexión, enviar solicitud, recibir respuesta y analizar datos
    """
    HOST = '192.168.0.180'
    PORT = 8234
    RETRY_INTERVAL = 1  # Intervalo de reintentos (segundos)
    MAX_RETRIES = 1  # Número máximo de reintentos

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Establecer el tiempo de espera de la conexión
            s.settimeout(5.0)
            s.connect((HOST, PORT))
            print(f"Conectado al servidor {HOST}:{PORT}")
            while True:
                device_values = []
                for device in device_sku:
                    addr = device['addr']
                    sku = device['sku']
                    retries = 0
                    while retries < MAX_RETRIES:
                        try:
                            # Enviar solicitud
                            check_val_flag = True
                            for i in range(0, 5):
                                request = create_request_message(addr)
                                s.send(request)
                                print(f"\nEnviar una solicitud al dispositivo {sku} (dirección: {addr}): {request.hex(' ')}")
                                # recibir una respuesta
                                response = receive_with_timeout(s)
                                print(f"Respuesta recibida: {response.hex(' ')}")
                                if not check_response_crc(response):
                                    print(f"Error al comprobar CRC, respuesta: {response.hex(' ')}")
                                    continue
                                # Análisis de los datos de respuesta
                                acceleration, velocity, displacement, temperature = parse_response_val(response)
                                # Imprimir resultados
                                print("\nResultado del análisis:")
                                print(f"Equipo SKU: {sku}--->{addr}")
                                print(f"aceleraciones: {acceleration:.1f} m/s²")
                                print(f"tempo: {velocity:.1f} mm/s")
                                print(f"desplazamiento (vector): {displacement} μm")
                                print(f"temp: {temperature:.2f} °C")
                                if (velocity < 0 or velocity > 200) or (temperature < 0 or temperature > 200):
                                    check_val_flag = False
                                    time.time(0.2)
                                    continue
                                else:
                                    check_val_flag = True
                                    break
                            if not check_val_flag:
                                break
                            # Análisis de los datos de respuesta
                            val1, val2, val3, val4, val5, val6, val7, val8 = parse_response(response, addr)
                            device_value = [addr, 3, 1, 5, val4, val5, val6, val7, val8]
                            device_values.append(device_value)
                            # Tras recibir y analizar correctamente los datos, espera un breve periodo de tiempo antes de enviar la siguiente solicitud.
                            time.sleep(0.2)
                            break  # Salir del bucle de reintento en caso de éxito
                        except (socket.timeout, ConnectionError) as e:
                            retries += 1
                            print(f"electrodomésticos {sku} Error de comunicación (intento) {retries}/{MAX_RETRIES}): {str(e)}")
                            if retries < MAX_RETRIES:
                                print(f"esperar {RETRY_INTERVAL} Reintentar después de segundos...")
                                time.sleep(RETRY_INTERVAL)
                            else:
                                print(f"electrodomésticos {sku} Comunicación fallida, continuar con el siguiente dispositivo")
                        except Exception as e:
                            print(f"electrodomésticos {sku} se produce un error: {e.args}")
                            break  # Otros errores omiten directamente el dispositivo actual
                if device_values:
                    print(f"result_json size is:{len(device_values)}")
                    # post_record(result_json=device_values, sku='YG100003')
    except ConnectionRefusedError:
        print("Conexión denegada, compruebe si la dirección del servidor y el puerto son correctos.")
    except Exception as e:
        print(f"se produce un error: {str(e)}")


if __name__ == "__main__":
    modbus_client()