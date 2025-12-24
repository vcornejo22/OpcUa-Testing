import random

MUTATION_SIZE = 0.1  # Tasa de mutación

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

def local_search(individual, rsa_bits, signature_algorithms, number_of_nodes_range, periods, target_cpu, target_memory, target_connection, target_subscription):
    # Intentar mejorar individual mutando en un solo parámetro y aceptando la mejora
    best_individual = individual
    best_fitness = fitness(*best_individual, target_cpu, target_memory, target_connection, target_subscription)
    
    for mutation_choice in ['rsa_bits', 'signature_algorithms', 'number_of_nodes', 'periods']:
        if mutation_choice == 'rsa_bits':
            for value in rsa_bits:
                mutated_individual = (value, individual[1], individual[2], individual[3])
                current_fitness = fitness(*mutated_individual, target_cpu, target_memory, target_connection, target_subscription)
                if current_fitness < best_fitness:
                    best_fitness = current_fitness
                    best_individual = mutated_individual
        elif mutation_choice == 'signature_algorithms':
            for value in signature_algorithms:
                mutated_individual = (individual[0], value, individual[2], individual[3])
                current_fitness = fitness(*mutated_individual, target_cpu, target_memory, target_connection, target_subscription)
                if current_fitness < best_fitness:
                    best_fitness = current_fitness
                    best_individual = mutated_individual
        elif mutation_choice == 'number_of_nodes':
            for value in number_of_nodes_range:
                mutated_individual = (individual[0], individual[1], value, individual[3])
                current_fitness = fitness(*mutated_individual, target_cpu, target_memory, target_connection, target_subscription)
                if current_fitness < best_fitness:
                    best_fitness = current_fitness
                    best_individual = mutated_individual
        elif mutation_choice == 'periods':
            for value in periods:
                mutated_individual = (individual[0], individual[1], individual[2], value)
                current_fitness = fitness(*mutated_individual, target_cpu, target_memory, target_connection, target_subscription)
                if current_fitness < best_fitness:
                    best_fitness = current_fitness
                    best_individual = mutated_individual
                    
    return best_individual

# Pseudocódigo del algoritmo genético memético
def genetic_algorithm(rsa_bits, signature_algorithms, number_of_nodes_range, periods, target_cpu, target_memory, target_connection, target_subscription, generations=100, population_size=20):
    # Inicializar la población
    population = [(random.choice(rsa_bits), random.choice(signature_algorithms), random.choice(number_of_nodes_range), random.choice(periods)) for _ in range(population_size)]
    
    for generation in range(generations):
        # Evaluar la población
        fitnesses = [fitness(*ind, target_cpu, target_memory, target_connection, target_subscription) for ind in population]
        
        # Nueva población
        new_population = []
        for _ in range(population_size // 2):
            # Selección
            parent1 = select(population, fitnesses)
            parent2 = select(population, fitnesses)
            
            # Cruce
            child1, child2 = crossover(parent1, parent2), crossover(parent2, parent1)
            
            # Mutación
            child1 = mutate(child1, rsa_bits, signature_algorithms, number_of_nodes_range, periods)
            child2 = mutate(child2, rsa_bits, signature_algorithms, number_of_nodes_range, periods)
            
            # Búsqueda local
            child1 = local_search(child1, rsa_bits, signature_algorithms, number_of_nodes_range, periods, target_cpu, target_memory, target_connection, target_subscription)
            child2 = local_search(child2, rsa_bits, signature_algorithms, number_of_nodes_range, periods, target_cpu, target_memory, target_connection, target_subscription)
            
            # Añadir a la nueva población
            new_population.extend([child1, child2])
        
        # Reemplazar la vieja población
        population = new_population
    
    # Devolver la mejor solución
    best_individual = min(population, key=lambda ind: fitness(*ind, target_cpu, target_memory, target_connection, target_subscription))
    return best_individual
