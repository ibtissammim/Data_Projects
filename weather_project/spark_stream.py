import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
#import spark


def create_keyspace(session):
    #create keyspace here
    session.execute(""" 
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '1'};
""")
    print("Keyspace created successfully!")

def create_table(session):
    session.execute(""" 
    CREATE TABLE IF NOT EXISTS spark_streams.weather_data (
        id UUID PRIMARY KEY,
        location_city TEXT,
        location_region TEXT,
        location_country TEXT,
        localtime TIMESTAMP,
        current_temp_c FLOAT,
        last_updated TIMESTAMP,
        description TEXT,
        icon TEXT,
        feelslike_c FLOAT,
        wind_kph FLOAT,
        wind_degree INT,
        pressure_mb FLOAT,
        humidity INT,
        heatindex_c FLOAT,
        dewpoint_c FLOAT,
        vis_km FLOAT,
        uv FLOAT,
        gust_kph FLOAT);
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    #insertion here
    print("inserting data...")

    id = kwargs.get('id')
    location_city = kwargs.get('location_city')
    location_region = kwargs.get('location_region')
    location_country = kwargs.get('location_country')
    localtime = kwargs.get('localtime')
    current_temp_c = kwargs.get('current_temp_c')
    last_updated = kwargs.get('last_updated')
    description = kwargs.get('description')
    icon = kwargs.get('icon')
    feelslike_c = kwargs.get('feelslike_c')
    wind_kph = kwargs.get('wind_kph')
    wind_degree = kwargs.get('wind_degree')
    pressure_mb = kwargs.get('pressure_mb')
    humidity = kwargs.get('humidity')
    heatindex_c = kwargs.get('heatindex_c')
    dewpoint_c = kwargs.get('dewpoint_c')
    vis_km = kwargs.get('vis_km')
    uv = kwargs.get('uv')
    gust_kph = kwargs.get('gust_kph')

    try:
        session.execute("""
            INSERT INTO spark_streams.weather_data(
            id,
            location_city,
            location_region,
            location_country,
            localtime,
            current_temp_c,
            last_updated,
            description,
            icon,
            feelslike_c,
            wind_kph,
            wind_degree,
            pressure_mb,
            humidity,
            heatindex_c,
            dewpoint_c,
            vis_km,
            uv,
            gust_kph
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    
        """, (
            id,
            location_city,
            location_region,
            location_country,
            localtime,
            current_temp_c,
            last_updated,
            description,
            icon,
            feelslike_c,
            wind_kph,
            wind_degree,
            pressure_mb,
            humidity,
            heatindex_c,
            dewpoint_c,
            vis_km,
            uv,
            gust_kph
        ))
        logging.info(f"Data inserted for {location_city} {localtime} ")
    except Exception as e :
        logging.error(f"failed insertion caused by this error : {e}")


def create_spark_connection():
    #creating spark connection 
    s_conn = None

    try:
        print("hopefully it will work")
        s_conn = SparkSession.builder\
            .appName('SparkDataStreaming')\
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
                    'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1')\
            .config('spark.cassandra.connection.host', 'cassandra')\
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to the exception {e}", exc_info=True)
    return s_conn

def create_cassandra_connection():
    #connection to cassandra cluster
    try :
        cluster = Cluster(contact_points=["cassandra"], port=9042, protocol_version=4)

        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create a Cassandra Connection due to the exception {e}")
        return None

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', 'broker:29092')\
            .option('subscribe', 'weather_data')\
            .option('startingOffsets', 'latest')\
            .option('kafka.group.id', 'spark-weather-group')\
            .load()
        logging.info("kafka dataframe created successfully!")
    except Exception as e :
        logging.error(f"enable to connect to kafka due to : {e}")

    return spark_df


def create_selection_df_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("location_city", StringType(), True),
        StructField("location_region", StringType(), True),
        StructField("location_country", StringType(), True),
        StructField("localtime", StringType(), True),  # or TimestampType() if formatted
        StructField("current_temp_c", FloatType(), True),
        StructField("last_updated", StringType(), True),  # or TimestampType()
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("feelslike_c", FloatType(), True),
        StructField("wind_kph", FloatType(), True),
        StructField("wind_degree", IntegerType(), True),
        StructField("pressure_mb", FloatType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("heatindex_c", FloatType(), True),
        StructField("dewpoint_c", FloatType(), True),
        StructField("vis_km", FloatType(), True),
        StructField("uv", FloatType(), True),
        StructField("gust_kph", FloatType(), True),
    ])


    json_df = spark_df.selectExpr("CAST(value AS STRING)")

    

    selected_df = (
        json_df
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("id", col("id").cast("string"))
    )

    
    return selected_df


def write_to_cassandra(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        print(f"\nReceived batch {batch_id} with {row_count} row(s)")

        if row_count == 0:
            print("Empty batch received. Nothing to write to Cassandra.\n")
            return

        print("Displaying first few rows from the batch:")
        batch_df.show(truncate=False)

        print("Writing batch to Cassandra...")
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", "spark_streams") \
            .option("table", "weather_data") \
            .save()

        print("Batch written to Cassandra successfully.\n")

    except Exception as e:
        print(f"\nFailed to write batch {batch_id} to Cassandra.")
        print(f"Error: {e}\n")





if __name__ == "__main__":
    #create spark connection
    print("the spark connection....")
    spark_conn = create_spark_connection()
    
    if spark_conn is not None :
        #connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn=spark_conn)
        selection_df = create_selection_df_kafka(spark_df=spark_df)
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Streaming is being started....")
            streaming_query = (
                selection_df.writeStream
                    .foreachBatch(write_to_cassandra)
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .start()
            )

            streaming_query.awaitTermination()
            
            
            

           
