import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import (StructField, StructType, IntegerType, StringType, ArrayType)

# Defines the level of logging and its format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Initialize spark session and limit to 1 worker
def spark_conn_init():
    try:
        conn = SparkSession.builder \
                .appName('SparkStreaming') \
                .config('spark.cassandra.connection.host', 'cassandra') \
                .config('spark.executor.instances', '1') \
                .config('spark.executor.cores', '2') \
                .config('spark.cores.max', '2') \
                .getOrCreate()

        conn.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection initialized')
        return conn
    except Exception as e:
        logging.error(f'Spark connection initialization failed: {e}')
        return None


# Connect spark to kafka and read topic
def read_from_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'pokemon_topic') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info('kafka stream loaded from topic')
        return spark_df

    except Exception as e:
        logging.error(f'Read from kafka error: {e}')
    return None


# Structure data from kafka topic
def structure_kafka_data(spark_df):
    schema = StructType([
        StructField('game_index', IntegerType(), True),
        StructField('name', StringType(), False),
        StructField('types', ArrayType(StringType()), False),
        StructField('base_experience', IntegerType(), False),
        StructField('height', IntegerType(), False),
        StructField('weight', IntegerType(), False),
        StructField('hp', IntegerType(), False),
        StructField('attack', IntegerType(), False),
        StructField('defense', IntegerType(), False),
        StructField('special_attack', IntegerType(), False),
        StructField('speed', IntegerType(), False)
    ])

    new_df = (
        spark_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("pokemon"))
        .select("pokemon.*")
        .withColumn('row_id', expr('uuid()'))
    )

    logging.info('Structured dataframe created from kafka stream')
    return new_df


# Create spark to cassandra session
def cassandra_connection():
    try:
        cass_conn = Cluster(['cassandra']).connect()
        logging.info('Connected to Cassandra cluster')
        return cass_conn

    except Exception as e:
        logging.error(f'Cassandra connection failed: {e}')
        return None


# Create cassandra keyspace for table
def create_keyspace(session, keyspace='spark_to_cassandra'):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
    """.format(keyspace=keyspace))

    logging.info(f'Created keyspace in cassandra: {keyspace}')


# Create cassandra table for incoming data
def create_table(session, keyspace = 'spark_to_cassandra', table = 'pokemon'):
    session.execute("""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
            row_id UUID PRIMARY KEY,
            game_index INT,
            name TEXT,
            types list<text>,
            base_experience INT,
            height INT,
            weight INT,
            hp INT,
            attack INT,
            defense INT,
            special_attack INT,
            speed INT
        );
    """.format(keyspace=keyspace, table=table))
    logging.info(f'Created table in cassandra: {table}')


# Calls functions to create sessions, read/structure dataframe and write to cassandra,
# only returns data where game index is not null, continuously streams data into cassandra.
if __name__ == "__main__":
    spark_conn = spark_conn_init()
    cass_session = cassandra_connection()

    if spark_conn and cass_session is not None:
        raw_df = read_from_kafka(spark_conn)
        structured_df = structure_kafka_data(raw_df)
        structured_df = structured_df.filter(col("game_index").isNotNull())
        create_keyspace(cass_session)
        create_table(cass_session)

        query = (structured_df.writeStream
                 .format("org.apache.spark.sql.cassandra")
                 .option('checkpointLocation', '/tmp/checkpoints')
                 .option('keyspace', 'spark_to_cassandra')
                 .option('table', 'pokemon')
                 .start())
        logging.info('Writing to Cassandra table: pokemon completed')

        query.awaitTermination()
