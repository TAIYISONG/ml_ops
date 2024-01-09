import datetime as dt
from datetime import datetime, timedelta
from os import read
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from inno_utils.azure import write_blob, read_blob
from inno_utils.loggers import log

from cltv.utils.configuration import context
from cltv.utils.session import spark


def get_active_customers():
    pass


def get_in_scope_customers(
    df_trx: SparkDataFrame, prior_date: str, in_scope_weeks: int
) -> SparkDataFrame:
    """
    Function responsible for targeting any customer that has purchase
    in the last 24 months (in_scope_weeks)
    Parameters
    ----------
    df_trx: SparkDataFrame. table with the transaction data
    prior_date: str, run_date -1
    in_scope_weeks: int, number of weeks we want to go back to consider a customer in scope

    Returns
    -------
    SparkDataFrame, list of customer that we want to include in the scope.
    """

    # Check all those customers that bought in the last in_scope_weeks (24 months)
    st_date = (prior_date - dt.timedelta(weeks=in_scope_weeks)).strftime("%Y-%m-%d")
    log.info(
        f"Selecting customer in scope as anyone that bought at least once after {st_date}"
    )
    df_in_scope = (
        df_trx.filter(F.col("SELLING_RETAIL_AMT") > 0)
        .filter(F.col("CALENDAR_DT") >= st_date)
        .select("CUSTOMER_CARD_ID")
        .distinct()
        .withColumn("IN_SCOPE", F.lit(1))
    )

    return df_in_scope


def add_new_customers_flag(
    df_in_scope: SparkDataFrame,
    df_trx: SparkDataFrame,
    prior_date: str,
    n_weeks_new: int,
) -> SparkDataFrame:
    """
    Function responsible for flagging new customers. Those customers have bought for the first
    time in the last n_weeks_new. For these customers we need to ensure they are not allocated
    in the control groups
    Parameters
    ----------
    df_in_scope: SparkDataFrame, list of customers that are in scope (last 24 months)
    df_trx: SparkDataFrame, transaction table
    prior_date: str, date of execution -1
    n_weeks_new: int, number of weeks we consider a customer new

    Returns
    -------
    SparkDataFrame
    """

    # find the new customers from the last 8 weeks
    st_date = (prior_date - dt.timedelta(weeks=n_weeks_new)).strftime("%Y-%m-%d")
    log.info(
        f"Labeling as new customer those that purchase for the first time after {st_date}"
    )
    df_new = (
        df_trx.filter(F.col("CALENDAR_DT") >= st_date)
        .select("CUSTOMER_CARD_ID")
        .distinct()
        .withColumn("IS_NEW", F.lit(1))
    )
    df_old = df_trx.filter(F.col("CALENDAR_DT") < st_date).select("CUSTOMER_CARD_ID")
    df_new = df_new.join(df_old, "CUSTOMER_CARD_ID", "left_anti")

    # Add the new customers to the scope table
    df_in_scope = df_in_scope.join(df_new, "CUSTOMER_CARD_ID", "left").fillna(
        0, "IS_NEW"
    )

    return df_in_scope
