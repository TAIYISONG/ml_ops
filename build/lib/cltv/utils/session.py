import imp
from inno_utils.loggers import log
import os
from typing import Tuple

from pyspark import SparkContext
from pyspark.sql import SparkSession

# Try creating the global spark and spark context
def _get_or_create_spark_session() -> Tuple[SparkSession, SparkContext]:
    # Create spark app name
    from cltv import run_id
    
    run_id = os.environ.get("RUN_ID", "adhoc")
    app_name = f"cltv_{run_id}"
    log.info(f"initializing spark session - {app_name}")
    # Create spark session
    spark_session = SparkSession.builder.appName(app_name).getOrCreate()
    log.getLogger("py4j").setLevel(log.ERROR)

    # Configure spark context
    sc = spark_session.sparkContext
    sc._jsc.hadoopConfiguration().setInt("fs.s3a.connection.maximum", 100)

    return spark_session, sc



try:
    spark, sc = _get_or_create_spark_session()
except Exception as e:
    log.error(e)
    log.error(
        (
            "Couldn't import spark...Running in an environment "
            "where Spark isn't properly configured"
        )
    )

    # TODO: WHY WOULD EVER ON EARTH YOU WOULD SET SPARK CONTEXT TO NONE????
    spark, sc = None, None