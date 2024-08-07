import os
import sys
from pyspark.sql import SparkSession

SPARK_VERSION = "3.4.0"
DELTA_VERSION = "2.4.0"
HADOOP_AWS_VERSION = "3.3.2"


def set_environment_variables():
    SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},io.delta:delta-core_2.12:{DELTA_VERSION} pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ["PYSPARK_PYTHON"] = sys.executable


def create_spark_session(app_name="DeltaReadExample"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000/")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    return spark


def read_delta_table(spark, delta_table_path):
    df = spark.read.format("delta").load(delta_table_path)
    return df


def main():
    set_environment_variables()
    spark = create_spark_session()
    delta_table_path = "s3a://huditest/hudidb/table_name=bronze_orders"
    df = read_delta_table(spark, delta_table_path)
    df.show()


if __name__ == "__main__":
    main()
