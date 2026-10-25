import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    count,
    date_format,
    avg,
    max,
    max_by,
    min,
    min_by,
    struct,
    to_json,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS = os.getenv("HDFS_CANDLES_PATH", "hdfs://namenode:9000/forex/candles")
PAIRS = ["EUR/USD", "USD/JPY", "EUR/JPY"]

tick_schema = StructType([
    StructField("pair", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("bid", DoubleType(), False),
    StructField("ask", DoubleType(), False),
    StructField("timestamp", StringType(), False),
])

spark = (
    SparkSession.builder.appName("forex-clean-candles")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "fx.ticks.raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

ticks = (
    raw.select(from_json(col("value").cast("string"), tick_schema).alias("tick"))
    .select("tick.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .filter(
        col("pair").isin(PAIRS)
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("bid").isNotNull()
        & col("ask").isNotNull()
        & (col("bid") > 0)
        & (col("ask") >= col("bid"))
        & col("event_time").isNotNull()
    )
)

cleaned_query = (
    ticks.select(
        col("pair").cast("string").alias("key"),
        to_json(struct(
            "pair",
            "price",
            "bid",
            "ask",
            "timestamp",
            "event_time",
        )).alias("value"),
    )
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("topic", "fx.ticks.cleaned")
    .option("checkpointLocation", "/tmp/spark-checkpoints/cleaned-ticks")
    .outputMode("append")
    .start()
)

bars = (
    ticks.withWatermark("event_time", "30 seconds")
    .groupBy("pair", window("event_time", "1 minute"))
    .agg(
        min_by(struct("event_time", "price"), "event_time").alias("first_tick"),
        max_by(struct("event_time", "price"), "event_time").alias("last_tick"),
        # FX quote data has no consolidated trade volume; count is the tick count.
        min("price").alias("low"),
        max("price").alias("high"),
        avg(col("ask") - col("bid")).alias("average_spread"),
        count("*").alias("tick_count"),
    )
    .select(
        col("pair"),
        col("window.start").alias("timestamp"),
        col("first_tick.price").alias("open"),
        col("high"),
        col("low"),
        col("last_tick.price").alias("close"),
        col("average_spread"),
        col("tick_count"),
    )
)

bars_json = bars.select(
    col("pair").alias("key"),
    to_json(struct(
        "pair",
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
        "open",
        "high",
        "low",
        "close",
        "average_spread",
        "tick_count",
    )).alias("value"),
)


def persist_candles(batch, batch_id):
    if batch.isEmpty():
        return
    batch.drop("key", "value").write.mode("append").partitionBy("pair").parquet(HDFS)


candles_kafka_query = (
    bars_json.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("topic", "fx.candles")
    .option("checkpointLocation", "/tmp/spark-checkpoints/candles-kafka")
    .outputMode("append")
    .start()
)
candles_hdfs_query = (
    bars.writeStream.foreachBatch(persist_candles)
    .option("checkpointLocation", "/tmp/spark-checkpoints/candles-hdfs")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
