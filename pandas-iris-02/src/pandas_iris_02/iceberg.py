from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def create_iceberg_table(parameters: Dict[str, Any]) -> None:
    schema = StructType([
        StructField("vendor_id", LongType(), True),
        StructField("trip_id", LongType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True)
    ])
    spark = SparkSession.getActiveSession()
    df = spark.createDataFrame([], schema)
    df.writeTo("demo.kedro02").create()