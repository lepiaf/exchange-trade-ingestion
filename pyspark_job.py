from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten, col, concat_ws, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

KAFKA_SERVER = ""
KAFKA_USERNAME = ""
KAFKA_PASSWORD = ""
KAFKA_TOPIC = ""
JAAS_CONFIG = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("StructuredKafkaWordCount")
        .getOrCreate()
    )

    # Create DataSet representing the stream of input lines from kafka
    lines = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS_CONFIG)
        .option("subscribe", KAFKA_TOPIC)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    schema = StructType([
        StructField("type", StringType()),
        StructField("product_id", StringType()),
        StructField("changes", ArrayType(ArrayType(StringType()))),
        StructField("time", TimestampType())
    ])

    df_json = (
        lines
        .withColumn(
            "jsonData",
            from_json(
                col("value"),
                schema,
            )
        )
        .select("jsonData.*")
    )

    query = (
        df_json
        .select(
            col("type"),
            col("product_id"),
            concat_ws("-", flatten("changes")),
            col("time"),
        )
        .writeStream
        .format('csv')
        .option("checkpointLocation", "output")
        .option("path", "output")
        .start()
    )

    query.awaitTermination()
