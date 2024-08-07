import os
import sys
from pyspark.sql import SparkSession

SPARK_VERSION = "3.4.0"
ICEBERG_VERSION = "0.12.0"
HADOOP_AWS_VERSION = "3.3.2"


def set_environment_variables():
    SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},org.apache.iceberg:iceberg-spark3-runtime:{ICEBERG_VERSION} pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
    os.environ["PYSPARK_PYTHON"] = sys.executable


def create_spark_session(app_name="IcebergReadExample"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.ic_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .getOrCreate()
    )

    # Setting S3 configurations
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:9000/")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    return spark


def read_iceberg_table(spark, iceberg_table_path):
    df = spark.read.format("iceberg").load(iceberg_table_path)
    return df


def main():
    try:
        set_environment_variables()
        spark = create_spark_session()
        iceberg_table_path = "s3a://huditest/hudidb/table_name=bronze_orders"
        df = read_iceberg_table(spark, iceberg_table_path)
        df.show()
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
