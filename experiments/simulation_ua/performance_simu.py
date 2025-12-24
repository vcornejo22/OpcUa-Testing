import asyncio
import logging
import os
import random
from datetime import datetime

import asyncua
import pandas as pd
from asyncua import Client, ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv
from influxdb_client import Dialect, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()
UA_URL = os.getenv("UA_URL")
UA_URI = os.getenv("UA_URI")
UA_USER = os.getenv("UA_USER")
UA_PASSWORD = os.getenv("UA_PASSWORD")

# Parámetros objetivo
TARGET_CPU = os.getenv("TARGET_CPU")  # Valor en porcentaje
TARGET_MEMORY = os.getenv("TARGET_MEMORY")  # Valor en MB
TARGET_CONNECTION_TIME = os.getenv("TARGET_CONNECTION_TIME") # Tiempo que demora en conectar al servidor en segundos
TARGET_SUBSCRIPTION_TIME = os.getenv("TARGET_SUBSCRIPTION_TIME") # Tiempo que demora en suscribir a los nodos
HOSTNAME = os.getenv("HOSTNAME")
# CONFIGURACIÓN AG
MUTATION_SIZE = os.getenv("MUTATION_SIZE")
POPULATION_SIZE = os.getenv("POPULATION_SIZE")
GENERATIONS = os.getenv("GENERATIONS")
TIMEOUT = 300

## INFLUX
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url_influx =  "http://192.168.1.250:8086"
bucket = os.getenv("INFLUXDB_BUCKET")

class GeneticAlgorithm:
    def __init__(self, target_cpu, target_memory, target_connection, target_subscription, mutation_size): 
        self.target_cpu = target_cpu
        self.target_memory = target_memory
        self.target_connection = target_connection
        self.target_subscription = target_subscription
        self.mutation_size = mutation_size

    def fitness(self, cpu_usage, memory_usage, connection_time, subscription_time):
        cpu_diff = abs(cpu_usage - float(self.target_cpu))
        memory_diff = abs(memory_usage - float(self.target_memory))
        connection_diff = abs(connection_time - float(self.target_connection))
        subscription_diff = abs(subscription_time - float(self.target_subscription))
        return cpu_diff + memory_diff + connection_diff + subscription_diff  # Menor es mejor

    def select(self, population, fitnesses, k=3):
        selected = random.choices(population, weights=[1/f for f in fitnesses], k=k)
        return min(selected, key=lambda ind: fitnesses[population.index(ind)])

    def crossover(self, parent1, parent2):
        return (parent1[0], parent2[1], parent1[2], parent2[3])

    def mutate(self, individual, rsa_bits, signature_algorithms, number_of_nodes_range, periods):
        if random.random() < self.mutation_size:  # Tasa de mutación
            mutation_choice = random.choice(['rsa_bits', 'signature_algorithms', 'number_of_nodes', 'periods'])
            if mutation_choice == 'rsa_bits':
                individual = (random.choice(rsa_bits), individual[1], individual[2], individual[3])
            elif mutation_choice == 'signature_algorithms':
                individual = (individual[0], random.choice(signature_algorithms), individual[2], individual[3])
            elif mutation_choice == 'number_of_nodes':
                individual = (individual[0], individual[1], random.choice(number_of_nodes_range), individual[3])
            elif mutation_choice == 'periods':
                individual = (individual[0], individual[1], individual[2], random.choice(periods))
        return individual
    
class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """
    async def datachange_notification(self, node, val, data):
        # print("Python: New data change event", node, val)
        pass

    def event_notification(self, event):
        print("Python: New event", event)


class OPCUAClient:
    def __init__(self, server_address, user, password):
        self.server_address = server_address
        self.user = user
        self.password = password
        self.client = Client(url=server_address, timeout=60*10)
        self.is_connected = False

    async def connect(self, cert, key, uri, number_nodes, period):
        try:
            await asyncio.sleep(5)
            init_time = datetime.now().timestamp()
            self.client.set_user(self.user)
            self.client.set_password(self.password)
            await self.client.set_security(
                security_policies.SecurityPolicyBasic256Sha256,
                certificate=cert,
                private_key=key,
                mode=ua.MessageSecurityMode.SignAndEncrypt
            )
            self.client.application_uri = uri
            await self.client.connect()
            self.is_connected = True
            connection_time = datetime.now().timestamp() - init_time
            var_list = [self.client.get_node(f"ns=2;i={i}") for i in range(1, number_nodes)]
            handler = SubHandler()
            sub = await self.client.create_subscription(period, handler)
            handle = await sub.subscribe_data_change(var_list)
            read_time = datetime.now().timestamp() - init_time
            await asyncio.sleep(TIMEOUT)
            print("Connected to server")
        except Exception as e:
            print(f"Failed to connect to server: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            self.is_connected = False
            await self.disconnect()
        return connection_time, read_time

    async def disconnect(self):
        await self.client.disconnect()
        self.is_connected = False
        print("Disconnected from server")

    async def send_data(self, data):
        if self.is_connected:
            try:
                node = self.client.get_node("ns=2;i=2")
                await node.write_value(data)
                print(f"Sent data: {data}")
            except Exception as e:
                print(f"Failed to send data: {e}")

    async def receive_data(self):
        if self.is_connected:
            try:
                node = self.client.get_node("ns=2;i=2")
                data = await node.read_value()
                print(f"Received data: {data}")
                return data
            except Exception as e:
                print(f"Failed to receive data: {e}")

    def adjust_parameters(self, params):
        self.current_params = params
        # Additional logic to adjust parameters on the server

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

    def process_csv_data(self, query, path_complete):
        dialect = Dialect(header=False, delimiter=',', comment_prefix='#', annotations=[], date_time_format="RFC3339")
        csv_result = self.query_handler.execute_query_csv(query, dialect)
        data_list = []
        for csv_line in csv_result:
            new_dict = {'timestamp': csv_line[5], 'value': csv_line[6], 'tag': csv_line[7]}
            data_list.append(new_dict)
            
        df = pd.DataFrame(data=data_list)
        df = df.pivot(index='timestamp', columns='tag', values='value').reset_index()
        df = df[['timestamp', 'cpu_percent', 'memory_percent', 'memory_used_mb', 'bandwidth_sent', 'bandwidth_recv']]
        df = df.rename(columns={"cpu_percent": "cpu_usage", "memory_used_mb": "memory_usage"})
        

        df.to_csv(f"{self.path_to_save}/{path_complete}.csv", index=False)
        return df

class InfluxMonitor:
    """
    Clase principal que combina la conexión, consultas y procesamiento de datos.
    """
    def __init__(self, url, token, org, hostname, path_to_save):
        self.influx_connection = InfluxDBConnection(url, token, org)
        influx_client = self.influx_connection.get_client()
        self.query_handler = InfluxDBQueryHandler(influx_client)
        self.data_processor = InfluxDataProcessor(self.query_handler, path_to_save)
        self.hostname = hostname

    def run(self, path_complete):
        query = f'''
        from(bucket: "Monitoring")
            |> range(start: -1m)
            |> filter(fn: (r) => r["_measurement"] == "system_metrics")
            |> filter(fn: (r) => r["host"] == "{self.hostname}")
        '''
        # Procesar datos en formato stream
        self.data_processor.process_stream_data(query)

        # Procesar datos en formato CSV
        self.data_processor.process_csv_data(query, path_complete)

       
async def main():
    rsa_bits = [1024, 2048, 3072, 4096]
    signature_algorithms = ["Sha1", "Sha224", "Sha256", "Sha384", "Sha512"]
    number_of_nodes = [1200, 1300, 1400, 1500, 1607]
    periods = [500, 1000, 1500, 2000, 2500, 3000]

    path_to_save = 'data_1211'

    # Inicializa la población
    population = [(random.choice(rsa_bits), random.choice(signature_algorithms), random.choice(number_of_nodes), random.choice(periods)) for _ in range(int(POPULATION_SIZE))]
    best_fitness = float('inf')
    best_individual = None
    
    df_con = pd.DataFrame()
    flag = True
    ag = GeneticAlgorithm(TARGET_CPU, TARGET_MEMORY, TARGET_CONNECTION_TIME, TARGET_SUBSCRIPTION_TIME, float(MUTATION_SIZE))
    
    process_monitor = InfluxMonitor(url_influx, token, org, HOSTNAME, path_to_save)
    
    for generation in range(int(GENERATIONS)):
        fitnesses = []
        for individual in population:
            rsa, signature, number_nodes, period = individual
            print(individual)
            opc_client = OPCUAClient(UA_URL, UA_USER, UA_PASSWORD)
            UA_KEY = f"opcua/{rsa}_{signature}/{rsa}_{signature}_private_key.pem"
            UA_CERT = f"opcua/{rsa}_{signature}/{rsa}_{signature}_certificate.pem"
            label = f"{generation}_{rsa}_{signature}"
            
            result = await asyncio.gather(opc_client.connect(UA_CERT, UA_KEY, UA_URI, number_nodes, period))
            process_monitor.run(label)
            c_time, r_time = result[0][0], result[0][1]
            
            df = pd.read_csv(f"{path_to_save}/{label}.csv")
            avg_cpu = df['cpu_usage'].mean()
            avg_memory = df['memory_usage'].mean()
            fitness_value = ag.fitness(avg_cpu, avg_memory, c_time, r_time)
            aux = pd.DataFrame.from_dict([{'timestamp': datetime.now(), 'generation': generation,'fitness': fitness_value , 'rsa': rsa, 'signature': signature, 'connect_time': c_time, 'read_time': r_time, 'number_nodes': number_nodes, 'period': period, 'avg_cpu': avg_cpu, 'avg_memory': avg_memory}])
            
            if flag:
                df_con = aux
                flag = False
            else:
                df_con = pd.concat([df_con, aux])
                
            fitnesses.append(fitness_value)
            print(f"Generation {generation}, Individual {rsa}_{signature}_{c_time}_{r_time}, Fitness: {fitness_value}")
            
            # Actualiza el mejor individuo si se encuentra uno mejor
            if fitness_value < best_fitness:
                best_fitness = fitness_value
                best_individual = individual
                
        # Selección y cruce
        new_population = []
        for _ in range(int(POPULATION_SIZE) // 2):
            parent1 = ag.select(population, fitnesses)
            parent2 = ag.select(population, fitnesses)
            offspring1 = ag.crossover(parent1, parent2)
            offspring2 = ag.crossover(parent2, parent1)
            new_population.extend([ag.mutate(offspring1, rsa_bits, signature_algorithms, number_of_nodes, periods), ag.mutate(offspring2, rsa_bits, signature_algorithms, number_of_nodes, periods)])

        population = new_population
        print(population)
    
    df_con.to_csv(f"{path_to_save}/data.csv", index=False)
      
    print(f"El mejor individuo es {best_individual} con un valor de fitness de {best_fitness}")

if __name__ == "__main__":
    asyncio.run(main())
