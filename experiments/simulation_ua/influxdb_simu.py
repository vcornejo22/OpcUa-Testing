import datetime
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

# Cargar las variables de entorno
load_dotenv()

# INFLUXDB configuraciones desde las variables de entorno
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url_influx = "http://192.168.1.250:8086"
bucket = os.getenv("INFLUXDB_BUCKET")

class InfluxDBConnection:
    """
    Singleton para la conexión a InfluxDB.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(InfluxDBConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self, url, token, org):
        if not hasattr(self, '_initialized'):
            self._client = InfluxDBClient(url=url, token=token, org=org, debug=False)
            self._initialized = True

    def get_client(self):
        return self._client

class InfluxDBQueryHandler:
    """
    Clase responsable de realizar consultas a InfluxDB y manejar los resultados.
    """
    def __init__(self, influx_client):
        self.query_api = influx_client.query_api()

    def execute_query(self, query):
        """Ejecuta una consulta y retorna los resultados como un stream."""
        return self.query_api.query_stream(query=query)

    def execute_query_csv(self, query, dialect):
        """Ejecuta una consulta y retorna los resultados en formato CSV."""
        return self.query_api.query_csv(query=query, dialect=dialect)

    def execute_query_dataframe(self, query):
        """Ejecuta una consulta y retorna los resultados como un DataFrame."""
        return self.query_api.query_data_frame(query=query)

class InfluxDataProcessor:
    """
    Clase encargada de procesar los resultados de las consultas a InfluxDB.
    """
    def __init__(self, query_handler, path_to_save):
        self.query_handler = query_handler
        self.path_to_save = path_to_save

    def process_stream_data(self, query):
        records = self.query_handler.execute_query(query)
        for record in records:
            # print(f"Value: {record}")
            pass

    def process_csv_data(self, query):
        dialect = Dialect(header=False, delimiter=',', comment_prefix='#', annotations=[], date_time_format="RFC3339")
        csv_result = self.query_handler.execute_query_csv(query, dialect)
        data_list = []
        for csv_line in csv_result:
            new_dict = {'timestamp': csv_line[5], 'value': csv_line[6], 'tag': csv_line[7]}
            data_list.append(new_dict)
            
        df = pd.DataFrame(data=data_list)
        df = df.pivot(index='timestamp', columns='tag', values='value').reset_index()
        df = df[['timestamp', 'cpu_percent', 'memory_percent', 'memory_used_mb', 'bandwidth_sent', 'bandwidth_recv']]

        df.to_csv(f"{self.path_to_save}.csv", index=False)
        return df

class InfluxMonitor:
    """
    Clase principal que combina la conexión, consultas y procesamiento de datos.
    """
    def __init__(self, url, token, org):
        self.influx_connection = InfluxDBConnection(url, token, org)
        influx_client = self.influx_connection.get_client()
        self.query_handler = InfluxDBQueryHandler(influx_client)
        self.data_processor = InfluxDataProcessor(self.query_handler)

    def run(self):
        query = '''
        from(bucket: "Monitoring")
            |> range(start: -1m)
            |> filter(fn: (r) => r["_measurement"] == "system_metrics")
            |> filter(fn: (r) => r["host"] == "server-ua")
        '''
        # Procesar datos en formato stream
        self.data_processor.process_stream_data(query)

        # Procesar datos en formato CSV
        self.data_processor.process_csv_data(query)

if __name__ == "__main__":
    monitor = InfluxMonitor(url_influx, token, org)
    monitor.run()
