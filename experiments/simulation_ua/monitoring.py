import psutil
import time
import asyncio
from dotenv import load_dotenv
from influxdb_client import Point
from influxdb_client.rest import ApiException
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import InfluxDBClient
from influxdb_client import PostBucketRequest, SchemaType
import os
import socket

# Cargar variables de entorno
load_dotenv()

## Configuración de InfluxDB
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url_influx = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")
interface = os.getenv("INTERFACE")
hostname = socket.gethostname()

class SystemMonitor:
    """
    Clase que gestiona el monitoreo de métricas del sistema (CPU, memoria, red).
    """

    def __init__(self, interface: str, interval: int = 1):
        self.interface = interface
        self.interval = interval

    async def get_metrics(self):
        """
        Obtiene las métricas de ancho de banda, uso de CPU y memoria.
        """
        try:
            net_io_1 = psutil.net_io_counters(pernic=True).get(self.interface)
            if net_io_1 is None:
                print(f"No se encontró la interfaz de red: {self.interface}")
                return

            # Esperar el intervalo especificado
            await asyncio.sleep(self.interval)

            # Obtener estadísticas luego del intervalo
            net_io_2 = psutil.net_io_counters(pernic=True).get(self.interface)

            # Calcular el ancho de banda (enviado/recibido por segundo en KB)
            bytes_sent = net_io_2.bytes_sent - net_io_1.bytes_sent
            bytes_recv = net_io_2.bytes_recv - net_io_1.bytes_recv
            bandwidth_sent_kbps = (bytes_sent / self.interval) / 1024
            bandwidth_recv_kbps = (bytes_recv / self.interval) / 1024

            # Calcular uso de CPU (%)
            cpu_percent = psutil.cpu_percent(interval=None)

            # Calcular uso de memoria (en porcentaje y MB)
            memory_info = psutil.virtual_memory()
            memory_percent = memory_info.percent
            # memory_used_mb = (memory_info.used + memory_info.buffers) / (1024 * 1024)
            memory_used_mb = memory_info.used / (1024 * 1024)

        except Exception as e:
            print(f"Error al obtener métricas del sistema: {e}")
            return None

        return {
            "bandwidth_sent": bandwidth_sent_kbps,
            "bandwidth_recv": bandwidth_recv_kbps,
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "memory_used_mb": memory_used_mb,
        }

class InfluxDBWriter:
    """
    Clase Singleton que gestiona la conexión y escritura de datos en InfluxDB.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(InfluxDBWriter, cls).__new__(cls)
        return cls._instance

    def __init__(self, url, token, org, bucket):
        if not hasattr(self, 'initialized'):  # Para evitar reinicialización
            self.url = url
            self.token = token
            self.org = org
            self.bucket = bucket
            self.client = None
            self.client_sync = None
            self.initialized = True

    async def connect(self):
        if self.client is None:
            self.client = InfluxDBClientAsync(url=self.url, token=self.token, org=self.org)
            self.client_sync = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.write_api = self.client.write_api()

    async def write(self, measurement: str, tags: dict, fields: dict):
        """
        Escribir un punto en InfluxDB.
        
        :param measurement: Nombre de la medición.
        :param tags: Etiquetas para identificar la fuente de los datos.
        :param fields: Valores de las métricas que se van a registrar.
        """
        point = Point(measurement).tag("host", tags["host"])
        for field_name, value in fields.items():
            point = point.field(field_name, value)
        
        try:
            await self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except ApiException as e:
            # self.create_bucket(bucket=self.bucket, org_name=self.org)
            print(f"Error al escribir en InfluxDB: {e}")

    def create_bucket(self, bucket: str , org_name: str):
        org_id = self.client_sync.organizations_api().find_organizations(org=org_name)[0].id
        create_bucket = self.client_sync.buckets_api().create_bucket(bucket=PostBucketRequest(name=bucket, org_id=org_id, retention_rules=[], schema_type=SchemaType.EXPLICIT))
        return create_bucket
        
    def find_bucket_by_name(self):
        return self.client_sync.buckets_api().find_bucket_by_name(bucket_name=self.bucket)
    
    async def close(self):
        if self.client:
            await self.client.__aexit__(None, None, None)

async def monitor_system(interface: str, interval: int):
    """
    Función principal para monitorear el sistema y enviar datos a InfluxDB.
    """
    monitor = SystemMonitor(interface, interval)
    influx_writer = InfluxDBWriter(url_influx, token, org, bucket)
    
    # Conectar a InfluxDB
    await influx_writer.connect()
    
    ## Busca el bucket y si no lo encuentra, crea un bucket
    find_bucket = influx_writer.find_bucket_by_name()
    if find_bucket == None:
        influx_writer.create_bucket(bucket, org)
    
    
    while True:
        metrics = await monitor.get_metrics()
        if metrics:
            # Enviar métricas a InfluxDB
            await influx_writer.write(
                measurement="system_metrics",
                tags={"host": hostname},
                fields=metrics
            )

async def main():
    interface = interface # "en0"  # Cambia esto según tu interfaz de red
    interval = 1  # Intervalo en segundos

    try:
        await monitor_system(interface, interval)
    except KeyboardInterrupt:
        print("Monitoreo interrumpido. Cerrando...")
    finally:
        influx_writer = InfluxDBWriter(url_influx, token, org, bucket)
        await influx_writer.close()

if __name__ == "__main__":
    asyncio.run(main())
