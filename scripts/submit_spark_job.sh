
========================================================
With OneTable
========================================================
--------------------
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
--------------------
jar tvf /home/foliveira/projects/personal/universal_lakehouse/infra/jars/hudi-utilities-slim-bundle_2.12-0.14.0.jar

========================================================
With METADATA AND INDEX
========================================================
spark-submit \
    --class org.apache.hudi.utilities.streamer.HoodieStreamer \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --properties-file '/home/foliveira/projects/personal/universal_lakehouse/infra/spark/spark-config.properties' \
    --master 'local[*]' \
    --executor-memory 1g \
    --jars hudi-extensions-0.1.0-SNAPSHOT-bundled.jar,hudi-java-client-0.14.0.jar,hudi-utilities-slim-bundle_2.12-0.14.0.jar \
    /home/foliveira/projects/personal/universal_lakehouse/infra/jars/hudi-utilities-slim-bundle_2.12-0.14.0.jar \
    --table-type COPY_ON_WRITE \
    --target-base-path 's3a://huditest/hudidb/table_name=bronze_orders' \
    --target-table bronze_orders \
    --op UPSERT \
    --enable-sync \
    --enable-hive-sync \
    --sync-tool-classes 'io.onetable.hudi.sync.OneTableSyncTool' \
    --source-limit 4000000 \
    --source-ordering-field ts \
    --source-class org.apache.hudi.utilities.sources.CsvDFSSource \
    --hoodie-conf 'hoodie.datasource.write.recordkey.field=order_id' \
    --hoodie-conf 'hoodie.datasource.write.partitionpath.field=state' \
    --hoodie-conf 'hoodie.datasource.write.precombine.field=ts' \
    --hoodie-conf 'hoodie.streamer.source.dfs.root=file://///home/foliveira/projects/personal/universal_lakehouse/data/raw/orders' \
    --hoodie-conf 'hoodie.deltastreamer.csv.header=true' \
    --hoodie-conf 'hoodie.deltastreamer.csv.sep=\t' \
    --hoodie-conf 'hoodie.onetable.formats.to.sync=DELTA,ICEBERG' \
    --hoodie-conf 'hoodie.onetable.target.metadata.retention.hr=168' \
    --hoodie-conf 'hoodie.metadata.index.async=true' \
    --hoodie-conf 'hoodie.metadata.enable=true' \
    --hoodie-conf 'hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.MultiPartKeysValueExtractor' \
    --hoodie-conf 'hoodie.datasource.hive_sync.metastore.uris=thrift://localhost:9083' \
    --hoodie-conf 'hoodie.datasource.hive_sync.mode=hms' \
    --hoodie-conf 'hoodie.datasource.hive_sync.enable=true' \
    --hoodie-conf 'hoodie.datasource.hive_sync.database=default' \
    --hoodie-conf 'hoodie.datasource.hive_sync.table=bronze_orders' \
    --hoodie-conf 'hoodie.datasource.write.hive_style_partitioning=true'


------------------------------------
RECORD LEVEL INDEX
------------------------------------------

spark-submit \
    --class org.apache.hudi.utilities.HoodieIndexer \
     --properties-file '/home/foliveira/projects/personal/universal_lakehouse/infra/spark/spark-config.properties' \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --master 'local[*]' \
    --executor-memory 1g \
    hudi-utilities-slim-bundle_2.12-0.14.0.jar \
     --mode scheduleAndExecute \
    --base-path 's3a://huditest/hudidb/table_name=bronze_orders' \
    --table-name bronze_orders \
    --index-types RECORD_INDEX \
    --hoodie-conf "hoodie.metadata.enable=true" \
    --hoodie-conf "hoodie.metadata.record.index.enable=true" \
    --hoodie-conf "hoodie.metadata.index.async=true" \
    --hoodie-conf "hoodie.write.concurrency.mode=optimistic_concurrency_control" \
    --hoodie-conf "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider" \
    --parallelism 2 \
    --spark-memory 2g


------------------------------------------------------
COLUMN_STATS INDEX
------------------------------------------------------------
MODE execute | schedule  | scheduleAndExecute

spark-submit \
    --class org.apache.hudi.utilities.HoodieIndexer \
    --properties-file '/home/foliveira/projects/personal/universal_lakehouse/infra/spark/spark-config.properties' \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --master 'local[*]' \
    --executor-memory 1g \
    hudi-utilities-slim-bundle_2.12-0.14.0.jar \
     --mode scheduleAndExecute \
    --base-path 's3a://huditest/hudidb/table_name=bronze_orders' \
    --table-name bronze_orders \
    --index-types COLUMN_STATS \
    --hoodie-conf "hoodie.metadata.enable=true" \
    --hoodie-conf "hoodie.metadata.index.async=true" \
    --hoodie-conf "hoodie.metadata.index.column.stats.enable=true" \
    --hoodie-conf "hoodie.write.concurrency.mode=optimistic_concurrency_control" \
    --hoodie-conf "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider" \
    --parallelism 2 \
    --spark-memory 2g

------------------------------------
cleaning
------------------------------------
spark-submit \
    --class org.apache.hudi.utilities.HoodieCleaner \
    --properties-file '/home/foliveira/projects/personal/universal_lakehouse/infra/spark/spark-config.properties' \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --master 'local[*]' \
    --executor-memory 1g \
    hudi-utilities-slim-bundle_2.12-0.14.0.jar \
    --target-base-path 's3a://huditest/hudidb/table_name=bronze_orders' \
    --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS \
    --hoodie-conf hoodie.cleaner.fileversions.retained=1 \
    --hoodie-conf hoodie.cleaner.parallelism=200
# =========================================
REGISTER THE TABLE IN HMS
# =========================================

spark-sql \
--packages 'io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2' \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.hadoop.fs.s3a.access.key=admin" \
--conf "spark.hadoop.fs.s3a.secret.key=password" \
--conf "spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "fs.s3a.signing-algorithm=S3SignerType" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083"

CREATE SCHEMA delta_db LOCATION 's3a://warehouse/';

CREATE TABLE delta_db.orders USING DELTA LOCATION 's3a://huditest/hudidb/table_name=bronze_orders';

# REGISTER THE TABLE IN HMS
# =========================================

spark-sql \
--packages 'org.apache.iceberg:iceberg-spark3-runtime:0.12.0,org.apache.hadoop:hadoop-aws:3.3.2' \
--conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.spark_catalog.type=hive" \
--conf "spark.sql.catalog.spark_catalog.uri=thrift://localhost:9083" \
--conf "spark.hadoop.fs.s3a.access.key=admin" \
--conf "spark.hadoop.fs.s3a.secret.key=password" \
--conf "spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000" \
--conf "spark.hadoop.fs.s3a.path.style.access=true" \
--conf "fs.s3a.signing-algorithm=S3SignerType" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083"

CREATE SCHEMA iceberg_db LOCATION 's3a://warehouse/';

CREATE OR REPLACE TABLE iceberg_db.orders USING ICEBERG LOCATION 's3a://huditest/hudidb/table_name=bronze_orders';
