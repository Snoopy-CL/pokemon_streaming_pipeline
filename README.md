# Pokemon Data Streaming Pipeline (Data Engineer Project)
## Project Overview
Pokemon Data Streaming Piepline is an end-to-end data engineering project designed to showcase practical skills and interest in modern data engineering. The project demonstrates hands-on experience with Kafka, Spark Structured Streaming, Cassandra, Airflow, Docker, and API ingestion.
The pipeline retrieves Pokemon data restricted to Pokemon available in Pokemon Emerald by continuously generating random Pokemon IDs for 60 seconds after each Airflow DAG trigger. Each generated ID is used to fetch Pokemon data from the PokeAPI, which is streamed to Kafka. Spark consumes the Kafka stream in real time, transforms the data, and writes it to Cassandra. Airflow orchestrates ingestion, and the entire system is containerized with Docker.

## Tech Stack
- Python
- Kafka
- Apache Spark (Structured Streaming)
- Cassandra
- PostgreSQL
- Apache Airflow
- Docker

## Continuous API Ingestion (Triggered by Airflow Daily)
- DAG triggers a script that generates random Pokémon IDs for one minute.
- Each ID fetches Pokémon data from the PokéAPI.
- Data is formatted and sent to Kafka (pokemon_topic).
- Only Pokémon appearing in Emerald are kept.
## Kafka Streaming Layer
- Kafka serves as the message bus for Pokémon data events.
- Spark listens to the topic for real-time processing.
## Real-Time Processing with Spark Structured Streaming
- Reads JSON messages from Kafka
- Parses and structures Pokémon data
- Filters out non-Emerald Pokémon
- Writes processed data to Cassandra
## Storage with Cassandra
- Stores structured Pokémon data using UUIDs as primary keys.
- Optimized for continuous streaming inserts.
## Orchestration with Airflow
- Triggers ingestion through a DAG (api_to_kafka).
- Metadata is stored in PostgreSQL.
## Dockerized Architecture
- Docker containers run all services: Kafka, Spark, Cassandra, Airflow, PostgreSQL.
- docker-compose manages networking, volumes, and dependencies.

## How to run (windows/bash)
1. Local directories must be changed to where app folder is located.
2. Start up docker with command "docker-compose up -d" and wait until airflow scheduler is done installing dependencies.
3. Start up spark_to_cassandra.py script by entering spark-master container using command "docker exec -it spark-master bash" and using command "spark-submit --master spark://spark-master:7077 spark_to_cassandra.py" to spark submit the streaming script.
4. On Docker Desktop, click the link to airflow webserver or enter http://localhost:8081 in the url.
5. Trigger DAG.
6. Open another terminal and enter cassandra using command "docker exec -it cassandra bash" then use command "cqlsh".
7. Enter keyspace using command "USE spark_to_cassandra;"
8. Query data from table pokemon using command "SELECT * FROM pokemon;" to view data.
9. If you would like to export the data, enter spark-master container using command "docker exec -it spark-master bash" then use command "spark-submit --master spark://spark-master:7077 export_pokemon_data.py"
10. To close everything, in terminal use command "docker-compose down"

## Summary of files
- "get_data.py": Fetches and streams Pokemon data to Kafka.
- "pokemon_data_output.csv": CSV export of Pokemon data from Cassandra.
- "entrypoint.sh": Initializes Airflow environment and starts webserver.
- "Dockerfile.spark": Custom Spark image with Kafka & Cassandra connectors.
- "docker-compose.yml": Launches all services and manages networking.
- "export_pokemon_data.py": Exports Cassandra data to CSV.
- "requirements.txt": Python dependencies for Spark and ingestion scripts.
- "spark_to_cassandra.py": Spark streaming job that reads from Kafka and writes to Cassandra.
