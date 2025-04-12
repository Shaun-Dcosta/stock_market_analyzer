from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, expr, to_json, struct
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType

# MySQL config
mysql_url = "jdbc:mysql://localhost:3306/stock_data"
mysql_props = {
    "user": "your_mysql_user",
    "password": "your_mysql_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Start Spark
spark = SparkSession.builder \
    .appName("StockKafkaProcessor") \
    .config("spark.jars", "/your/path/to/mysql-connector-java-8.0.xx.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka input
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-prices-raw") \
    .option("startingOffsets", "latest") \
    .load()

# Schema for input JSON
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", FloatType()) \
    .add("volume", IntegerType()) \
    .add("timestamp", StringType())

parsed_df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        col("data.volume"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    )

# Write raw stream to MySQL
def write_to_mysql(batch_df, _):
    batch_df.write.jdbc(url=mysql_url, table="stock_prices", mode="append", properties=mysql_props)

parsed_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

# 1️⃣ Aggregated Stats (15-min tumbling window)
agg_df = parsed_df \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "15 minutes"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price"),
        min("price").alias("min_price"),
        avg("volume").alias("avg_volume")
    ).select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("max_price"),
        col("min_price"),
        col("avg_volume")
    )

# Output to stock-prices-aggregated topic
agg_kafka = agg_df.selectExpr("to_json(struct(*)) AS value")

agg_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock-prices-aggregated") \
    .outputMode("update") \
    .start()

# 2️⃣ Alert Detection Logic
# Join with aggregation to compare current price with avg
alert_df = parsed_df.alias("current").join(
    agg_df.alias("agg"),
    on="symbol"
).filter(
    (col("current.price") > col("agg.avg_price") * 1.03) |
    (col("current.volume") > col("agg.avg_volume") * 1.5)
).select(
    col("current.symbol"),
    col("current.price"),
    col("agg.avg_price"),
    col("current.volume"),
    col("agg.avg_volume"),
    col("current.timestamp").alias("alert_time")
)

# Format alert as JSON
alert_kafka = alert_df.select(to_json(struct("*")).alias("value"))

alert_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock-alerts") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
