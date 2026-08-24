import os

from pyspark.sql import SparkSession

# 保險:YAML 沒設時的 fallback,設了則不覆蓋
os.environ.setdefault("HADOOP_USER_NAME", "bigred")

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("TDCS_TravelTime_API")
            .master(os.getenv("SPARK_MASTER", "yarn"))
            .config("spark.submit.deployMode", "client")
            .config("spark.yarn.jars", "hdfs:///spark/jars-3.5.1/*")
            .config("spark.driver.host", os.environ["POD_IP"])
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "40000")
            .config("spark.blockManager.port", "40001")
            .config("spark.ui.port", "4040")
            .config("spark.driver.memory",
                    os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory",
                    os.getenv("SPARK_EXECUTOR_MEMORY", "5g"))
            .config("spark.executor.cores",
                    os.getenv("SPARK_EXECUTOR_CORES", "8"))
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "2")
            .config("spark.dynamicAllocation.maxExecutors", "9")
            .config("spark.dynamicAllocation.executorIdleTimeout", "120s")
            .config("spark.sql.shuffle.partitions",
                    os.getenv("SPARK_SHUFFLE_PARTITIONS", "24"))
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
    return _spark
