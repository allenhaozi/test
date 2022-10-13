"""
 Copyright 2022 4Paradigm

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("S3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.access.key", "deltalake")
    .config("spark.hadoop.fs.s3a.secret.key", "deltalake")
    .config("spark.hadoop.fs.s3a.endpoint", "172.27.70.11:9000")
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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# spark.sparkContext.setLogLevel("DEBUG")

local_data_dir = "userdata1.parquet"
s3_table_path = "s3a://deltalake/tutorial/userdata"

s3_table = "s3a://deltalake/video/demo-01"


def write_data_from_hdfs_to_s3():
    data = spark.read.format("parquet").load(local_data_dir)
    data.write.format("delta").mode("overwrite").save(s3_table_path)


def get_data_from_s3():
    # data = spark.read.format("delta").load(s3_table_path)
    # data.show()

    # dd = spark.sql(f"select * from delta.`{s3_table}` limit 20")
    # df = spark.read.format("delta").load(s3_table)
    # dd.show()

    # cnt = spark.sql(f"select count(*) from delta.`{s3_table_path}`")
    cnt = spark.sql(f"select * from delta.`{s3_table_path}` limit 1")

    cnt.show()


if __name__ == "__main__":
    write_data_from_hdfs_to_s3()
    get_data_from_s3()
