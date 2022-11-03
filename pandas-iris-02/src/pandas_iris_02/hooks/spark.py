import os

import pyspark
from delta import *
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHooks:
    @hook_impl
    def after_context_created(self, context: KedroContext) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        # TODO: deltalake / iceberg
        self.build_iceberg_spark_session(context)
        
        
        
    def build_iceberg_spark_session(self, context: KedroContext): 
        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        # kerberos
        enable_kerberos = False
        if enable_kerberos:
            principal = ""
            # TODO: how to set 
            # System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);
            spark_conf.set('hadoop.security.authorization', True)
            spark_conf.set('hadoop.security.authentication', 'kerberos')
            spark_conf.set('dfs.namenode.kerberos.principal', principal)
            spark_conf.set('dfs.datanode.kerberos.principal', principal)

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(context._package_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")


    def build_deltalake_spark_session():
        #TODO: how to pass the s3 configuration
        endpoint = os.getenv('S3_DEV_ENDPOINT')

        builder = (
            SparkSession.builder.appName("S3")
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

