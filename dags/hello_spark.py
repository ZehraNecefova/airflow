from pyspark.sql import SparkSession
from pyspark.sql.functions import col,current_date, datediff,unix_timestamp
from datetime import datetime as dt

spark = SparkSession.builder \
    .appName("ProcessingTask") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    ])) \
    .config("spark.hadoop.fs.s3a.access.key", "telcoaz") \
    .config("spark.hadoop.fs.s3a.secret.key", "Telco12345") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


df = spark.read.parquet("s3a://spark/yellow_tripdata_2024-01.parquet")

df = df.withColumn(
    "trip_duration_hours",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
)
df_filtered = df.filter(col("trip_duration_hours") > 1)
df_narrow = df_filtered.select("PULocationID", "DOLocationID", "trip_duration_hours")

df_grouped = df_narrow.groupBy("PULocationID").count()
df_joined = df_narrow.join(df_grouped, on="PULocationID", how="left")

df_joined.write.mode("overwrite").parquet(f"s3a://spark/{dt.today()}")
