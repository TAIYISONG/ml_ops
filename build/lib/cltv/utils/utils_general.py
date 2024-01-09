from datetime import datetime, timedelta
from os import read
from typing import Optional, Union

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

from inno_utils.azure import write_blob, read_blob
from inno_utils.loggers import log

from cltv.utils.configuration import context
from cltv.utils.session import spark


def add_store_physical_store_location_no(
    df: SparkDataFrame,
    location_df: Optional[SparkDataFrame] = None,
    drop: Optional[bool] = False
) -> SparkDataFrame: 
    """
    Add STORE_PHYSICAL_LOCATION_NO column to df

    Parameters
    ----------
    df: SparkDataFrame
        df to get STORE_PHYSICAL_LOCATION_NO column
    location_df: SparkDataFrame
        location table containing STORE_PHYSICAL_LOCATION_NO to RETAIL_OUTLET_LOCATION_SK mapping
    drop: bool
        whether to drop RETAIL_OUTLET_LOCATION_SK in input df
    Returns
    -------
        SparkDataFrame: df with STORE_PHYSICAL_LOCATION_NO column added
    """

    if "STORE_PHYSICAL_LOCATION_NO" in df.columns:
        print("STORE_PHYSICAL_LOCATION_NO is presented")
    else:
        msg = "RETAIL_OUTLET_LOCATION_SK column not in df"
        assert "RETAIL_OUTLET_LOCATION_SK" in df.columns, msg

        if not location_df:
            location_path = context.config["data_inputs"]["store_hier_path"]
            location_df = read_blob(spark, location_path)
            location_df = (
                location_df
                .select("STORE_PHYSICAL_LOCATION_NO", "RETAIL_OUTLET_LOCATION_SK")
                .distinct()
                # .filter(~F.col("STORE_PHYSICAL_LOCATION_NO").isNull())
            )

        df = df.join(
                location_df,
                df.RETAIL_OUTLET_LOCATION_SK.cast("int")
                == location_df.RETAIL_OUTLET_LOCATION_SK.cast("int"),
                how="left",
            ).drop(location_df.RETAIL_OUTLET_LOCATION_SK)

        if drop:
            df = df.drop("RETAIL_OUTLET_LOCATION_SK")
    
    return df