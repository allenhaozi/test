import os

import pyspark
from delta import *
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """

        # Load the spark configuration in spark.yaml using the config loader
        # parameters = context.config_loader.get("spark*", "spark*/**")
        # spark_conf = SparkConf().setAll(parameters.items())

        endpoint = os.getenv('S3_DEV_ENDPOINT')

        builder = (
            pyspark.sql.SparkSession.builder.appName("S3")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.access.key", "deltalake")
            .config("spark.hadoop.fs.s3a.secret.key", "deltalake")
            .config("spark.hadoop.fs.s3a.endpoint", f"{endpoint}")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
            .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", True)
            .config("spark.hadoop.fs.s3a.path.style.access", True)
            .config("spark.hadoop.fs.s3a.attempts.maximum", 3)
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", 30000)
            .config("spark.hadoop.fs.s3a.connection.timeout", 30000)
            .config("spark.hadoop.fs.s3a.paging.maximum", 100)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
        )


        _spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
