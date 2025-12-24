import asyncio
import os
import random
from datetime import datetime

import pandas as pd
import psutil
from asyncua import Client, ua
from asyncua.crypto import security_policies
from dotenv import load_dotenv

load_dotenv()
UA_URL = os.getenv("UA_URL")
UA_URI = os.getenv("UA_URI")
UA_USER = os.getenv("UA_USER")
UA_PASSWORD = os.getenv("UA_PASSWORD")
PID = int(os.getenv("PID"))

# Parámetros objetivo
TARGET_CPU = os.getenv("TARGET_CPU")  # Valor en porcentaje
TARGET_MEMORY = os.getenv("TARGET_MEMORY")  # Valor en MB
TARGET_CONNECTION_TIME = os.getenv(
    "TARGET_CONNECTION_TIME"
)  # Tiempo que demora en conectar al servidor en segundos
TARGET_SUBSCRIPTION_TIME = os.getenv(
    "TARGET_SUBSCRIPTION_TIME"
)  # Tiempo que demora en suscribir a los nodos

# CONFIGURACIÓN AG
MUTATION_SIZE = os.getenv("MUTATION_SIZE")
POPULATION_SIZE = os.getenv("POPULATION_SIZE")
GENERATIONS = os.getenv("GENERATIONS")
TIMEOUT = 300


class GeneticAlgorithm:
    def __init__(
        self,
        target_cpu,
        target_memory,
        target_connection,
        target_subscription,
        mutation_size,
    ):
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
        return (
            cpu_diff + memory_diff + connection_diff + subscription_diff
        )  # Menor es mejor

    def select(self, population, fitnesses, k=3):
        selected = random.choices(population, weights=[1 / f for f in fitnesses], k=k)
        return min(selected, key=lambda ind: fitnesses[population.index(ind)])

    def crossover(self, parent1, parent2):
        return (parent1[0], parent2[1], parent1[2], parent2[3])

    def mutate(
        self, individual, rsa_bits, signature_algorithms, number_of_nodes_range, periods
    ):
        if random.random() < self.mutation_size:  # Tasa de mutación
            mutation_choice = random.choice(
                ["rsa_bits", "signature_algorithms", "number_of_nodes", "periods"]
            )
            if mutation_choice == "rsa_bits":
                individual = (
                    random.choice(rsa_bits),
                    individual[1],
                    individual[2],
                    individual[3],
                )
            elif mutation_choice == "signature_algorithms":
                individual = (
                    individual[0],
                    random.choice(signature_algorithms),
                    individual[2],
                    individual[3],
                )
            elif mutation_choice == "number_of_nodes":
                individual = (
                    individual[0],
                    individual[1],
                    random.choice(number_of_nodes_range),
                    individual[3],
                )
            elif mutation_choice == "periods":
                individual = (
                    individual[0],
                    individual[1],
                    individual[2],
                    random.choice(periods),
                )
        return individual


class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """

    async def datachange_notification(self, node, val, data):
        print("Python: New data change event", node, val)
        # pass

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
                mode=ua.MessageSecurityMode.SignAndEncrypt,
            )
            self.client.application_uri = uri
            await self.client.connect()
            self.is_connected = True
            connection_time = datetime.now().timestamp() - init_time
            var_list = [
                self.client.get_node(f"ns=3;i={i}") for i in range(1001, number_nodes)
            ]
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


class ProcessMonitor:
    def __init__(self, pid, timeout):
        self.pid = pid
        self.timeout = timeout
        self.process = psutil.Process(pid)
        self.df = pd.DataFrame()

    async def read_process(self, csv_filename, path_to_save):
        flag = True
        count = 0
        memory_init = 0
        try:
            while count < self.timeout + 20:
                # Obtener el uso de CPU como porcentaje
                cpu_usage = self.process.cpu_percent(interval=0)
                # Obtener el uso de memoria
                memory_info = self.process.memory_info()
                memory_usage = memory_info.rss / (1024 * 1024)  # Convertir a MB
                # Obtener el tiempo actual
                memory_end = memory_usage - memory_init
                current_time = datetime.now()
                aux = pd.DataFrame.from_dict(
                    [
                        {
                            "timestamp": current_time,
                            "cpu_usage": cpu_usage,
                            "memory_usage": memory_end,
                        }
                    ]
                )
                if flag:
                    self.df = aux
                    memory_init = memory_info.rss / (1024 * 1024)
                    flag = False
                else:
                    self.df = pd.concat([self.df, aux])
                await asyncio.sleep(0.2)
                count += 0.2
        except TypeError:
            pass
        finally:
            self.df.to_csv(f"{path_to_save}/{csv_filename}.csv", index=False)
        return datetime.now()


async def main():
    rsa_bits = [1024, 2048, 3072, 4096]
    signature_algorithms = ["Sha1", "Sha224", "Sha256", "Sha384", "Sha512"]
    number_of_nodes = [1200, 1300, 1400, 1500, 1607]
    periods = [500, 1000, 1500, 2000, 2500, 3000]

    path_to_save = "data_1411"

    # Inicializa la población
    population = [
        (
            random.choice(rsa_bits),
            random.choice(signature_algorithms),
            random.choice(number_of_nodes),
            random.choice(periods),
        )
        for _ in range(int(POPULATION_SIZE))
    ]
    best_fitness = float("inf")
    best_individual = None

    df_con = pd.DataFrame()
    flag = True
    ag = GeneticAlgorithm(
        TARGET_CPU,
        TARGET_MEMORY,
        TARGET_CONNECTION_TIME,
        TARGET_SUBSCRIPTION_TIME,
        float(MUTATION_SIZE),
    )

    process_monitor = ProcessMonitor(PID, TIMEOUT)

    for generation in range(int(GENERATIONS)):
        fitnesses = []
        for individual in population:
            rsa, signature, number_nodes, period = individual
            print(individual)
            opc_client = OPCUAClient(UA_URL, UA_USER, UA_PASSWORD)
            UA_KEY = f"opcua/{rsa}_{signature}/{rsa}_{signature}_private_key.pem"
            UA_CERT = f"opcua/{rsa}_{signature}/{rsa}_{signature}_certificate.pem"
            label = f"{generation}_{rsa}_{signature}_{number_nodes}_{period}"
            pid = int(PID)
            result = await asyncio.gather(
                opc_client.connect(UA_CERT, UA_KEY, UA_URI, number_nodes, period),
                process_monitor.read_process(label, path_to_save),
            )

            c_time, r_time = result[0][0], result[0][1]

            df = pd.read_csv(f"{path_to_save}/{label}.csv")
            avg_cpu = df["cpu_usage"].mean()
            avg_memory = df["memory_usage"].mean()
            fitness_value = ag.fitness(avg_cpu, avg_memory, c_time, r_time)
            aux = pd.DataFrame.from_dict(
                [
                    {
                        "timestamp": datetime.now(),
                        "generation": generation,
                        "fitness": fitness_value,
                        "rsa": rsa,
                        "signature": signature,
                        "connect_time": c_time,
                        "read_time": r_time,
                        "number_nodes": number_nodes,
                        "period": period,
                        "avg_cpu": avg_cpu,
                        "avg_memory": avg_memory,
                    }
                ]
            )

            if flag:
                df_con = aux
                flag = False
            else:
                df_con = pd.concat([df_con, aux])

            fitnesses.append(fitness_value)
            print(
                f"Generation {generation}, Individual {rsa}_{signature}_{c_time}_{r_time}, Fitness: {fitness_value}"
            )

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
            new_population.extend(
                [
                    ag.mutate(
                        offspring1,
                        rsa_bits,
                        signature_algorithms,
                        number_of_nodes,
                        periods,
                    ),
                    ag.mutate(
                        offspring2,
                        rsa_bits,
                        signature_algorithms,
                        number_of_nodes,
                        periods,
                    ),
                ]
            )

        population = new_population
        print(population)

    df_con.to_csv(f"{path_to_save}/data.csv", index=False)

    print(
        f"El mejor individuo es {best_individual} con un valor de fitness de {best_fitness}"
    )


if __name__ == "__main__":
    asyncio.run(main())
