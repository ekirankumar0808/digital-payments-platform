```python
import os
import uuid
from datetime import datetime

from py4j.java_gateway import java_import
from delta.tables import DeltaTable

from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    sha2,
    concat_ws,
    to_date
)

from spark.schemas.transaction_schema import transaction_schema
from spark.utils.config_loader import load_config
from spark.utils.logger import get_logger
from spark.utils.spark_session import create_spark_session
from spark.utils.audit_logger import write_audit_log


class BronzeIngestionJob:

    def __init__(self):

        # --------------------------------------------------
        # ENV + CONFIG
        # --------------------------------------------------

        self.env = os.getenv("ENV", "dev")
        self.config = load_config(self.env)

        # --------------------------------------------------
        # LOGGER
        # --------------------------------------------------

        self.logger = get_logger(
            logger_name="BronzeIngestion",
            log_path=self.config['logging']['log_path'],
            log_level=self.config['logging']['log_level']
        )

        # --------------------------------------------------
        # PATHS
        # --------------------------------------------------

        self.raw_path = self.config['paths']['raw']
        self.bronze_path = self.config['paths']['bronze']
        self.metadata_path = self.config['paths']['bronze_metadata']
        self.audit_path = self.config['paths']['audit_pipeline_runs']

        # --------------------------------------------------
        # SPARK SESSION
        # --------------------------------------------------

        self.spark = create_spark_session("BronzeIngestion")
        self.spark.sparkContext.setLogLevel("ERROR")

        self.logger.info("BronzeIngestionJob Initialized")

    # --------------------------------------------------
    # FILESYSTEM
    # --------------------------------------------------

    def get_filesystem(self):

        hadoop_conf = self.spark._jsc.hadoopConfiguration()

        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
        java_import(self.spark._jvm, "java.net.URI")

        raw_path_obj = self.spark._jvm.Path(self.raw_path)

        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            self.spark._jvm.URI(self.raw_path),
            hadoop_conf
        )

        return fs, raw_path_obj

    # --------------------------------------------------
    # LIST FILES
    # --------------------------------------------------

    def get_raw_files(self):

        self.logger.info(f"Checking raw path: {self.raw_path}")

        fs, raw_path_obj = self.get_filesystem()

        if not fs.exists(raw_path_obj):
            raise Exception(f"Raw path does not exist: {self.raw_path}")

        files = fs.listStatus(raw_path_obj)

        csv_files = []

        for file in files:

            file_path = file.getPath().toString()

            if "paysim" in file_path and file_path.endswith(".csv"):

                csv_files.append({
                    "file_path": file_path,
                    "file_name": file.getPath().getName(),
                    "mod_time": file.getModificationTime()
                })

        return csv_files

    # --------------------------------------------------
    # NEW FILES FILTER
    # --------------------------------------------------

    def get_new_files(self, csv_files):

        if len(csv_files) == 0:

            self.logger.warning("No CSV files found in raw layer")
            return []

        csv_df = self.spark.createDataFrame(csv_files)

        try:

            processed_df = (
                self.spark.read
                .format("delta")
                .load(self.metadata_path)
                .filter("status = 'SUCCESS'")
                .select("file_path")
                .distinct()
            )

            self.logger.info("Loaded metadata table successfully")

            new_files_df = csv_df.join(
                processed_df,
                on="file_path",
                how="left_anti"
            )

        except Exception as e:

            self.logger.warning(
                f"Metadata table missing. Initial load. Error: {str(e)}"
            )

            new_files_df = csv_df

        return [row.asDict() for row in new_files_df.collect()]

    # --------------------------------------------------
    # READ CSV
    # --------------------------------------------------

    def read_csv(self, file_path):

        return (
            self.spark.read
            .format("csv")
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(transaction_schema)
            .load(file_path)
        )

    # --------------------------------------------------
    # EXTRACT INGESTION DATE
    # --------------------------------------------------

    def extract_ingestion_date(self, file_name):

        try:

            date_part = (
                file_name
                .replace("paysim_transactions_", "")
                .replace(".csv", "")
            )

            return datetime.strptime(date_part, "%Y%m%d").date()

        except Exception:

            raise Exception(
                f"Invalid file naming format: {file_name}"
            )

    # --------------------------------------------------
    # WRITE BRONZE
    # --------------------------------------------------

    def write_bronze(self, df):

        (
            df.repartition(4)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("ingestion_date")
            .save(self.bronze_path)
        )

    # --------------------------------------------------
    # METADATA UPDATE
    # --------------------------------------------------

    def update_metadata(self, file, row_count, status, pipeline_run_id):

        metadata_row = Row(
            pipeline_run_id=pipeline_run_id,
            file_name=file["file_name"],
            file_path=file["file_path"],
            modification_time=file["mod_time"],
            row_count=row_count,
            status=status,
            ingestion_timestamp=datetime.now()
        )

        metadata_df = self.spark.createDataFrame([metadata_row])

        if DeltaTable.isDeltaTable(self.spark, self.metadata_path):

            delta_table = DeltaTable.forPath(
                self.spark,
                self.metadata_path
            )

            (
                delta_table.alias("target")
                .merge(
                    metadata_df.alias("source"),
                    "target.file_path = source.file_path"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            (
                metadata_df.write
                .format("delta")
                .mode("overwrite")
                .save(self.metadata_path)
            )

    # --------------------------------------------------
    # PROCESS FILE
    # --------------------------------------------------

    def process_file(self, file):

        pipeline_run_id = str(uuid.uuid4())

        self.logger.info(
            f"[RUN_ID={pipeline_run_id}] "
            f"Processing file: {file['file_path']}"
        )

        # ------------------------------------------
        # METADATA STARTED
        # ------------------------------------------

        self.update_metadata(
            file=file,
            row_count=0,
            status="STARTED",
            pipeline_run_id=pipeline_run_id
        )

        # ------------------------------------------
        # READ CSV
        # ------------------------------------------

        df = self.read_csv(file["file_path"])

        # ------------------------------------------
        # PERSIST DATAFRAME
        # ------------------------------------------

        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # ------------------------------------------
        # ROW COUNT
        # ------------------------------------------

        row_count = df.count()

        # ------------------------------------------
        # EMPTY FILE CHECK
        # ------------------------------------------

        if row_count == 0:

            self.logger.warning(
                f"[RUN_ID={pipeline_run_id}] "
                f"Empty file detected: {file['file_name']}"
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="bronze",
                run_id=pipeline_run_id,
                status="FAILED",
                records_processed=0,
                error_message="Empty file"
            )

            self.update_metadata(
                file=file,
                row_count=0,
                status="FAILED",
                pipeline_run_id=pipeline_run_id
            )

            df.unpersist()

            return

        self.logger.info(
            f"[RUN_ID={pipeline_run_id}] "
            f"Row count: {row_count}"
        )

        # ------------------------------------------
        # INGESTION DATE
        # ------------------------------------------

        ingestion_date = self.extract_ingestion_date(
            file["file_name"]
        )

        self.logger.info(
            f"[RUN_ID={pipeline_run_id}] "
            f"Ingestion date: {ingestion_date}"
        )

        # ------------------------------------------
        # TRANSFORMATIONS
        # ------------------------------------------

        df = (
            df.withColumn(
                "transaction_id",
                sha2(
                    concat_ws(
                        "||",
                        "step",
                        "type",
                        "amount",
                        "nameOrig",
                        "nameDest"
                    ),
                    256
                )
            )
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "processing_timestamp",
                current_timestamp()
            )
            .withColumn(
                "source_file",
                lit(file["file_path"])
            )
            .withColumn(
                "ingestion_date",
                to_date(lit(ingestion_date))
            )
        )

        # ------------------------------------------
        # WRITE BRONZE
        # ------------------------------------------

        try:

            self.write_bronze(df)

            self.logger.info(
                f"[RUN_ID={pipeline_run_id}] "
                f"Bronze write completed"
            )

        except Exception as e:

            self.logger.error(
                f"[RUN_ID={pipeline_run_id}] "
                f"Bronze write failed: {str(e)}"
            )

            write_audit_log(
                spark=self.spark,
                audit_path=self.audit_path,
                pipeline_name="digital_payments_pipeline",
                layer="bronze",
                run_id=pipeline_run_id,
                status="FAILED",
                records_processed=0,
                error_message=str(e)
            )

            self.update_metadata(
                file=file,
                row_count=0,
                status="FAILED",
                pipeline_run_id=pipeline_run_id
            )

            df.unpersist()

            raise e

        # ------------------------------------------
        # AUDIT LOG
        # ------------------------------------------

        write_audit_log(
            spark=self.spark,
            audit_path=self.audit_path,
            pipeline_name="digital_payments_pipeline",
            layer="bronze",
            run_id=pipeline_run_id,
            status="SUCCESS",
            records_processed=row_count
        )

        # ------------------------------------------
        # METADATA SUCCESS
        # ------------------------------------------

        self.update_metadata(
            file=file,
            row_count=row_count,
            status="SUCCESS",
            pipeline_run_id=pipeline_run_id
        )

        # ------------------------------------------
        # UNPERSIST
        # ------------------------------------------

        df.unpersist()

    # --------------------------------------------------
    # RUN PIPELINE
    # --------------------------------------------------

    def run(self):

        self.logger.info("Starting Bronze Ingestion")

        csv_files = self.get_raw_files()

        new_files = self.get_new_files(csv_files)

        if not new_files:

            self.logger.info("No new files found")

            return

        self.logger.info(
            f"New files identified: {len(new_files)}"
        )

        for file in new_files:

            try:

                self.process_file(file)

            except Exception as e:

                self.logger.error(
                    f"Failed file {file['file_name']}: {str(e)}"
                )

                write_audit_log(
                    spark=self.spark,
                    audit_path=self.audit_path,
                    pipeline_name="digital_payments_pipeline",
                    layer="bronze",
                    run_id=str(uuid.uuid4()),
                    status="FAILED",
                    records_processed=0,
                    error_message=str(e)
                )

        self.logger.info("Bronze Ingestion Completed")

    # --------------------------------------------------
    # STOP SPARK
    # --------------------------------------------------

    def stop(self):

        self.logger.info("Stopping Spark")

        self.spark.stop()


# --------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------

if __name__ == "__main__":

    job = BronzeIngestionJob()

    try:

        job.run()

    except Exception as e:

        job.logger.error(f"Fatal error: {str(e)}")

        raise

    finally:

        job.stop()
```
