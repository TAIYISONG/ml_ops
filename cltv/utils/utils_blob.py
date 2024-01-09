""" 
TODO: Functions in this module could be later moved into Inno-Utils steam. DE could do a review.
"""

#%%
import datetime as dt
import hashlib
import os
import re
import shutil
import time
import traceback
import uuid
from pathlib import Path
from typing import List, Optional, Tuple, Union
from re import findall
from inspect import getframeinfo, currentframe

import pandas as pd

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from inno_utils.azure import write_blob, read_blob
from inno_utils.loggers import log

import cltv
from cltv.utils.configuration import context
from cltv.utils.session import spark


def exists_blob(spark: SparkSession, path: str) -> bool:
    """
    Checks if dataset exists and can be read by attempting to read it.
    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    True if dataset exists and can be read, False otherwise
    """


    if path.endswith(".csv"):
        func = spark.read.csv
        kwargs = {"path": path, "header": True}
    else:
        func = read_blob
        kwargs = {"spark": spark, "path": path}

    try:
        func(**kwargs)
        return True
    except Exception:
        return False



def backup_on_blob(
    spark: SparkSession,
    df: SparkDataFrame,
    path: Optional[str] = None,
    partition_by: Union[str, list] = None
):

    # get the name of the argument to "df" parameter
    _, _, _, code_context, _ = getframeinfo(currentframe().f_back)
    _, *parameter = findall("\w+", code_context[0])

    saving_name = [x for x in parameter if "df" in x]
    if len(saving_name) > 0:
        df_name = saving_name[0]
    else: 
        df_name = "saved_df"

    # specify saving path
    path = path or context.config["project_root_paths"] + f"/temp_backup/{df_name}"

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    suffix = cltv.run_id + "_" + ts
    path_to_write = path + "_" + suffix

    log.info(f"Try saving {df_name} under {path_to_write}")

    msg = (
        "You are overwriting a backup! Pass a different 'backup_name' "
        "or don't pass one for a random string"
    )

    assert not exists_blob(spark, path_to_write), msg

    log.info(f"Caching data here: {path_to_write}")

    write_blob(
        spark=spark,
        df=df,
        path=path_to_write,
        file_format="parquet",
        writemode="overwrite",
        partitionby=partition_by,
    )

    is_empty = read_blob(spark=spark, path=path_to_write).limit(1).rdd.isEmpty()
    msg = f"Empty data was written: {path_to_write}"
    assert not is_empty, msg

    return read_blob(spark, path_to_write)


# for testing
if __name__ == '__main__':
    from pyspark.sql import Row
    from datetime import datetime, date

    sample_df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])

    backup_on_blob(spark, sample_df, path="/adhoc/oliver/test_data")