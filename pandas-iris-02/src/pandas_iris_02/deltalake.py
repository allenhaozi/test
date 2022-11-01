from typing import Any, Dict

from pyspark.sql import SparkSession

hdfs_dir = "hdfs://172.27.70.11:8020/demo/parquet/userdata1.parquet"
s3_table_path = "s3a://deltalake/tutorial/kedro"

#s3_table = "s3a://kedro/tutorial/userdata"


def deltalake_task(parameters: Dict[str,Any]) -> None:
    spark = SparkSession.getActiveSession()
    data = spark.read.format("parquet").load(hdfs_dir)
    data.show()
    data.write.format("delta").mode("overwrite").save(s3_table_path)
