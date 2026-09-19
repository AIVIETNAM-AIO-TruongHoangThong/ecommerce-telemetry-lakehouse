"""
common/spark_session.py
-----------------------
TODO: Build and return a configured SparkSession.
      Load settings from configs/spark_config.yaml.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ecommerce-pipeline") -> SparkSession:
    # TODO: Read spark_config.yaml
    # TODO: Configure driver/executor memory, cores, extra JARs
    # TODO: Return SparkSession
    raise NotImplementedError
