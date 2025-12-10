from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Create spark session connected to cassandra. Limit to 1 worker
spark_conn = SparkSession.builder \
            .appName('Export_Cassandra') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .config('spark.executor.instances', '1') \
            .config('spark.executor.cores', '2') \
            .config('spark.cores.max', '2') \
            .getOrCreate()

# Read pokemon table from cassandra to dataframe
df = spark_conn.read \
    .format('org.apache.spark.sql.cassandra') \
    .options(table='pokemon', keyspace='spark_to_cassandra') \
    .load()

# Convert types array to comma separated string
df = df.withColumn("types", concat_ws(",", col("types")))

# write dataframe to single csv into exports folder
df.coalesce(1).write \
    .option('header', 'true') \
    .mode('overwrite') \
    .csv('/exports/pokemon_data')

print('Export to csv complete')