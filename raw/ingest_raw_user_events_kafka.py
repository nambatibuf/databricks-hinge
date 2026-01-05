from typing import Any, Dict, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def dedupe_kafka_exactly_once(batch_df: DataFrame) -> DataFrame:
    """
    Dedupe within each micro-batch using Kafka coordinates.
    This removes duplicates caused by retries / replays within the same batch.

    Key:
      (topic, partition, offset) is unique for a record in Kafka.
    """
    return batch_df.dropDuplicates(["topic", "partition", "offset"])


def bronze_batch_append(
    batch_df: DataFrame,
    batch_id: int,
    *,
    catalog: str,
    schema: str,
    table: str,
) -> None:
    full_table = f"{catalog}.{schema}.{table}"

    df = dedupe_kafka_exactly_once(batch_df)

    (df.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(full_table)
    )

def run_kafka_ec2_to_delta_bronze(
    *,
    ec2_public_ip: str,
    port: int,
    topic: str,
    catalog: str,
    schema: str,
    table: str,
    checkpoint_location: str,
    starting_offsets: str = "earliest",  # for first backfill; switch to "latest" for ongoing
    fail_on_data_loss: str = "false",
) -> StreamingQuery:

    bootstrap = f"{ec2_public_ip}:{port}"

    kafka_options: Dict[str, Any] = {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "failOnDataLoss": fail_on_data_loss,
        # optional tuning (safe defaults):
        # "kafka.request.timeout.ms": "60000",
        # "kafka.session.timeout.ms": "30000",
    }

    # I am reading from Kafka
    sdf = (
        spark.readStream
             .format("kafka")
             .options(**kafka_options)
             .load()
    )

    # raw value + kafka metadata
    bronze_df = (
        sdf.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("key_str"),
            F.col("value").cast("string").alias("value_str"),  #my events
            F.current_timestamp().alias("ingest_ts"),
        )
    )

    query = (
        bronze_df.writeStream
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            .trigger(availableNow=True)
            .foreachBatch(lambda df, bid: bronze_batch_append(
                df, bid, catalog=catalog, schema=schema, table=table
            ))
            .start()
    )

    return query


# ---------------- Example usage (EC2 Kafka) ----------------
query = run_kafka_ec2_to_delta_bronze(
    ec2_public_ip="35.160.162.96",     # your broker advertised.listeners public IP
    port=9092,
    topic="hinge-user-visits",
    catalog="raw",
    schema="app",
    table="user_visits",
    checkpoint_location="/Volumes/hinge_dev/s3/hinge_datalake/checkpoints/user_visits",
    starting_offsets="earliest",
)

# query.awaitTermination()
