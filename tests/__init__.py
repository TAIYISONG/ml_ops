
def get_spark_for_test():
    from pyspark.sql.session import SparkSession

    spark = (
        SparkSession.builder.appName("Your App Name")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.databricks.service.server.enabled", "true")
        .getOrCreate()
    )
    """
    conf = spark.sparkContext._conf.setAll(
        [
            ("spark.driver.memory", "8g"),
            ("spark.executor.memory", "8g"),
            ("spark.sql.autoBroadcastJoinThreshold", "-1"),
        ]
    )

    spark.sparkContext.stop()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    """
    return spark


class SparkTests(object):
    _spark = None

    def __new__(cls, *args, **kwargs):
        if not cls._spark:
            print("new")
            cls._spark = get_spark_for_test()

        return cls._spark


spark = SparkTests()