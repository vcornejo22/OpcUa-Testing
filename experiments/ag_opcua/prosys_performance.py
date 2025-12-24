import asyncio
import os
import random
from datetime import datetime
from itertools import product

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
TARGET_CONNECTION_TIME = os.getenv("TARGET_CONNECTION_TIME") # Tiempo que demora en conectar al servidor en segundos
TARGET_SUBSCRIPTION_TIME = os.getenv("TARGET_SUBSCRIPTION_TIME") # Tiempo que demora en suscribir a los nodos

# CONFIGURACIÓN AG
MUTATION_SIZE = os.getenv("MUTATION_SIZE")
POPULATION_SIZE = os.getenv("POPULATION_SIZE")
GENERATIONS = os.getenv("GENERATIONS")
TIMEOUT = 300

# Crear el directorio 'data' si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Algoritmo Genético
def fitness(cpu_usage, memory_usage, connection_time, subscription_time, target_cpu, target_memory, target_connection, target_subscription):
    cpu_diff = abs(cpu_usage - float(target_cpu))
    memory_diff = abs(memory_usage - float(target_memory))
    connection_diff = abs(connection_time - float(target_connection))
    subscription_diff = abs(subscription_time - float(target_subscription))
    return cpu_diff + memory_diff + connection_diff + subscription_diff  # Menor es mejor

def select(population, fitnesses, k=3):
    selected = random.choices(population, weights=[1/f for f in fitnesses], k=k)
    return min(selected, key=lambda ind: fitnesses[population.index(ind)])

def crossover(parent1, parent2):
    return (parent1[0], parent2[1])

def mutate(individual, rsa_bits, signature_algorithms, number_of_nodes_range, periods):
    if random.random() < MUTATION_SIZE:  # Tasa de mutación
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


# Lee el consumo CPU y RAM
async def read_process(csv_filename, pid):
    p = psutil.Process(pid)
    df = pd.DataFrame()
    flag = True
    count = 0
    try:
        while count < TIMEOUT + 20:
            # Obtener el uso de CPU como porcentaje
            cpu_usage = p.cpu_percent(interval=0)
            # Obtener el uso de memoria
            memory_info = p.memory_info()
            memory_usage = memory_info.rss  # Resident Set Size
            memory_usage = memory_usage / (1024 * 1024)
            # print(f"Time: {datetime.now()} CPU Usage: {cpu_usage}% - Memory Usage: {memory_usage} MB")
            current_time = datetime.now()
            aux = pd.DataFrame.from_dict([{'timestamp': current_time, 'cpu_usage': cpu_usage, 'memory_usage': memory_usage}])
            if flag:
                df = aux
                flag = False
            else:
                df = pd.concat([df, aux])
            await asyncio.sleep(0.2)
            count += 0.2

    except TypeError:
        pass
    finally:
        df.to_csv(f"data/{csv_filename}.csv", index=False)
    return datetime.now()

# Client UA
class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    """
    async def datachange_notification(self, node, val, data):
        # print("Python: New data change event", node, val)
        pass

    def event_notification(self, event):
        print("Python: New event", event)

async def connect_ua(UA_CERT, UA_KEY, rsa, number_nodes, period):
    await asyncio.sleep(5)
    init_time = datetime.now().timestamp()
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
        end_time = datetime.now().timestamp()
        connection_time = end_time - init_time
        var_list = [ua_client.get_node(f"ns=3;i={i}") for i in range(1001, number_nodes)]
        handler = SubHandler()
        sub = await ua_client.create_subscription(period, handler)
        handle = await sub.subscribe_data_change(var_list)
        read_time = datetime.now().timestamp() - init_time
        # while True:
        await asyncio.sleep(TIMEOUT)
    except KeyboardInterrupt:
        pass
    finally:
        await ua_client.disconnect()
    return connection_time, read_time

async def main():
    rsa_bits = [1024, 2048, 3072, 4096]
    signature_algorithms = ["Sha1", "Sha224", "Sha256", "Sha384", "Sha512"]
    number_of_nodes = [1200, 1300, 1400, 1500, 1607]
    periods = [500, 1000, 1500, 2000, 2500, 3000]
    population_size = POPULATION_SIZE
    generations = GENERATIONS

    # Inicializa la población
    population = [(random.choice(rsa_bits), random.choice(signature_algorithms), random.choice(number_of_nodes), random.choice(periods)) for _ in range(population_size)]
    best_fitness = float('inf')
    best_individual = None
    
    df_con = pd.DataFrame()
    flag = True
    
    for generation in range(generations):
        fitnesses = []
        for individual in population:
            rsa, signature, number_nodes, period = individual
            print(individual)
            UA_KEY = f"opcua/{rsa}_{signature}/{rsa}_{signature}_private_key.pem"
            UA_CERT = f"opcua/{rsa}_{signature}/{rsa}_{signature}_certificate.pem"
            label = f"{generation}_{rsa}_{signature}"
            pid = int(PID)
            result = await asyncio.gather(connect_ua(UA_CERT, UA_KEY, label, number_nodes, period), read_process(label, pid))
            
            c_time, r_time = result[0][0], result[0][1]
            aux = pd.DataFrame.from_dict([{'timestamp': datetime.now(),'rsa': rsa, 'connect_time': c_time, 'read_time': r_time}])
            
            df = pd.read_csv(f"data/{label}.csv")
            avg_cpu = df['cpu_usage'].mean()
            avg_memory = df['memory_usage'].mean()
            fitness_value = fitness(avg_cpu, avg_memory, c_time, r_time, TARGET_CPU, TARGET_MEMORY, TARGET_CONNECTION_TIME, TARGET_SUBSCRIPTION_TIME)
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
        for _ in range(int(population_size) // 2):
            parent1 = select(population, fitnesses)
            parent2 = select(population, fitnesses)
            offspring1 = crossover(parent1, parent2)
            offspring2 = crossover(parent2, parent1)
            new_population.extend([mutate(offspring1, rsa_bits, signature_algorithms, number_of_nodes, periods), mutate(offspring2, rsa_bits, signature_algorithms, number_of_nodes, periods)])

        population = new_population
    
    df_con.to_csv(f"data/data.csv", index=False)
      
    print(f"El mejor individuo es {best_individual} con un valor de fitness de {best_fitness}")

if __name__ == "__main__":
    asyncio.run(main())
