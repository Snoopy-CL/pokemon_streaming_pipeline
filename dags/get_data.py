import requests
import json
from airflow.decorators import dag, task
from datetime import datetime
from kafka import KafkaProducer
import time
import random
import logging

# Defines name and start date of DAG
default_args = {
    'owner': 'owner',
    'start_date': datetime(2025, 12, 1)
}

# Defines the level of logging and its format
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)

# Gets pokemon data randomly for 60 seconds from an api
def get_data():
    start = time.monotonic()

    try:
        count = requests.get('https://pokeapi.co/api/v2/pokemon?limit=0', timeout = 5).json()['count']
    except (requests.RequestException, ValueError) as e:
        logging.error(f'Error fetching pokemon count: {e}')
        return

    while time.monotonic() - start < 60:
        pokemon_id = random.randint(1, count)
        url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}'

        try:
            response = requests.get(url, timeout = 5).json()
            yield response

        except (requests.RequestException, ValueError) as e:
            logging.error(f'Error fetching pokemon id {pokemon_id}: {e}')
            continue

        time.sleep(1)

# Takes raw pokemon data and only keeps relevant fields then formats into final dictionary
def format_data(response):
    game_index_emerald = None
    for i in response['game_indices']:
        if i['version']['name'] == 'emerald':
            game_index_emerald = i['game_index']
            break

    stats = {}
    for i in response['stats']:
        name = i['stat']['name']
        value = i['base_stat']
        stats[name] = value

    types = []
    if 'types' in response:
        for i in response['types']:
            if 'type' in i and 'name' in i['type']:
                types.append(i['type']['name'])

    data = {}
    data['game_index'] = game_index_emerald
    data['name'] = response['name']
    data['types'] = types
    data['base_experience'] = response['base_experience']
    data['height'] = response['height']
    data['weight'] = response['weight']
    data['hp'] = stats.get('hp')
    data['attack'] = stats.get('attack')
    data['defense'] = stats.get('defense')
    data['special_attack'] = stats.get('special-attack')
    data['speed'] = stats.get('speed')

    return data

# Brings the functions above together to stream the data into kafka topic
def stream_data_kafka():
    producer = KafkaProducer(
        bootstrap_servers=['broker:9092'],
        max_block_ms=5000
    )

    logging.info("Started streaming chunked Pokémon data to Kafka")

    for response in get_data():
        formatted = format_data(response)
        json_data = json.dumps(formatted).encode("utf-8")
        producer.send("pokemon_topic", json_data)

    producer.flush()
    logging.info("Finished streaming chunked Pokémon data to Kafka")

# Defines airflow DAG and default arguments
@dag(
    dag_id = 'api_to_kafka',
    default_args = default_args,
    schedule = '@daily',
    catchup = False
)
def api_to_kafka():
    # Calls stream data to kafka function
    @task(task_id='get_data')
    def task_1():
        stream_data_kafka()

    task_1()

# DAG variable to be used
my_dag = api_to_kafka()