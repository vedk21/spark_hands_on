from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType

def consume_customer_kafka_stream(kafka_brokers, kafka_topic, group_id):
    """
    Consumes a Kafka stream of customer data using Spark Structured Streaming,
    processes it to count customers per city, and prints the results.
    """

    spark = SparkSession.builder \
        .appName("CustomerKafkaStreamConsumer") \
        .getOrCreate()

    # --- Define Customer Data Schema ---
    customer_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("order_count", IntegerType()),
        StructField("registration_timestamp", LongType()) # Timestamp is sent as Long (seconds since epoch)
    ])

    # --- Kafka Source Configuration ---
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("group.id", group_id) \
        .option("startingOffsets", "latest") \
        .load()

    # --- Deserialize JSON and Parse Customer Data ---
    customer_data_df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), customer_schema).alias("customer")) \
        .select("customer.*") # Flatten the nested struct


    # --- Data Processing: Count Customers by City ---
    city_customer_counts_df = customer_data_df \
        .groupBy("city") \
        .count() \
        .orderBy("city") # Optional: Order results by city name

    # --- Output Sink: Print Customer Counts for Each Batch ---
    def process_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            print(f"----- Batch: {batch_id} - Customer Counts by City -----")
            batch_df.show() # Print the DataFrame for this batch
            # Alternative: Print each row more explicitly
            # batch_df.foreach(lambda row: print(f"City: {row.city}, Count: {row['count']}"))
            print(f"----- Batch: {batch_id} - End -----")

    query = city_customer_counts_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(process_batch) \
        .start()
     # "complete" mode is suitable for aggregations that update

    query.awaitTermination()


if __name__ == "__main__":
    # --- Configure your Kafka and Spark Streaming parameters here ---
    kafka_brokers_config = "localhost:9092"  # Replace with your Kafka broker(s)
    kafka_topic_config = "customer_topic"     # Should match producer topic
    group_id_config = "customer_consumer_group" # Choose a unique group ID

    consume_customer_kafka_stream(kafka_brokers_config, kafka_topic_config, group_id_config)