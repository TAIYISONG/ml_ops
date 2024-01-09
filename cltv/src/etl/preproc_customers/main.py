from datetime import datetime, timedelta
from os import read
from typing import Optional

from pyspark.sql import DataFrame

from inno_utils.azure import write_blob, read_blob
from inno_utils.loggers import log

from cltv.utils.configuration import context
from cltv.utils.session import spark
from cltv.utils.decorators import timeit

from cltv.src.etl.preproc_customers.helpers import (
    get_in_scope_customers,
    add_new_customers_flag
)



@timeit
def task_create_cust_status(write: Optional[bool] = True) -> DataFrame:
    """
    Responsible of creating the status of each customers. This can be:
    - Active: has purchase in the last 12 weeks
    - In scope: has purchase in the last 24 months
    - New: is new customer from airmiles
    """

    # dataset paths
    path_df_trx = context.config["preprocess_data_outputs"]["core_feature_txn"]
    # read datasets:
    df_trx = read_blob(spark, path_df_trx)

    # read in parameters
    in_scope_weeks = context.config["status_cust"]["in_scope_weeks"]
    new_weeks = context.config["status_cust"]["new_weeks"]
    prior_date = context.prior_date

    # get customers that are in the previous 24 months
    df_in_scope = get_in_scope_customers(df_trx, prior_date, in_scope_weeks)

    # get customer that are new
    df_in_scope = add_new_customers_flag(
        df_in_scope, df_trx, prior_date, n_weeks_new=new_weeks
    )

    if write:
        context.data.write("customer_status", df=df_in_scope, break_on_writing=False)

    return df_in_scope