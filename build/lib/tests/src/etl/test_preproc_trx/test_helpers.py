import pytest

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark() -> SparkSession:

    from tests import spark

    return spark


