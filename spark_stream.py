from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .getOrCreate()

# Define schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", StringType())

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Convert Kafka value
json_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Calculate average price
avg_df = parsed_df.groupBy("symbol").avg("price")

# Output to console
query = avg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()