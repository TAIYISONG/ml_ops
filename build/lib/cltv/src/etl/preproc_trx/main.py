from datetime import datetime, timedelta
from os import read
from typing import Optional

from pyspark.sql import DataFrame

from inno_utils.azure import write_blob, read_blob
from inno_utils.loggers import log

from cltv.utils.configuration import context, NUM
from cltv.utils.session import spark
from cltv.utils.decorators import timeit
from cltv.utils.utils_general import add_store_physical_store_location_no
from cltv.utils.utils_calendar import adweek_generate
from cltv.src.etl.preproc_trx.helpers import clean_txn, add_cogs, add_txn_loc

# NUM = 1000


@timeit
def task_process_trx(
    write: Optional[bool] = True,
) -> DataFrame:
    """
    Task responsible for creating a clean version of transaction table. This includes
    - Filtering dates
    - Adding product hierarchy
    - Adding customer_card_id
    - Adding
    Function responsible of creating the clean txn data for the active customer cold start
    run
    Parameters
    ----------
    write: bool, if True we write out the output
    """

    # get parameters
    print("**********", NUM)
    region = context.region
    prior_date = context.prior_date
    print("*************run date", context.run_date)
    print("*************prior date", context.prior_date)
    data_inputs = context.config["data_inputs"]
    data_outputs = context.config["preprocess_data_outputs"]
    params_txn = context.config["txn_preprocess"]
    sel_cols = params_txn["sel_cols"]
    params_active_cust = context.config["active_cust"]
    n_weeks = (
        params_active_cust["n_weeks"]
        + params_active_cust["n_backfill_cold_start_weeks"]
    )

    # dataset paths
    path_raw_trx = data_inputs["transactions_path"][region]
    path_cust_ids = data_inputs["cust_ids"]
    path_calendar = data_inputs["calendar"]
    path_prod_hier = data_inputs["item_hier_path"]
    path_location = data_inputs["store_hier_path"]
    path_cogs = data_inputs["cogs_path"][region]
    path_preprocess_txn = data_outputs["core_feature_txn"]

    # # reading inputs
    # df_trx_raw = read_blob(spark, path_raw_trx)
    # df_cust_ids = read_blob(spark, path_cust_ids)
    # df_cal = read_blob(spark, path_calendar)
    # df_prod_hier = read_blob(spark, path_prod_hier)
    # df_loc = read_blob(spark, path_location)
    # df_cogs = read_blob(spark, path_cogs)

    # scope for the transactional data, i.e. start/end dates
    prior_date = datetime.strptime(prior_date, "%Y-%m-%d")
    st_date = (prior_date - timedelta(weeks=n_weeks)).strftime("%Y-%m-%d")

    msg = f"""
    Using the following inputs:
     - path_raw_trx = '{path_raw_trx}'
     - path_cust_ids = '{path_cust_ids}'
     - path_prod_hier = '{path_prod_hier}'
     - prior_date = '{prior_date}'
     - start_date = '{st_date}'
     - end_date = '{prior_date}'
     - region = '{region}'

    To produce output:
     - output_data_path = '{path_preprocess_txn}'
    """
    log.info(msg)

    # df_trx = clean_txn(
    #     df_txn=df_trx_raw,
    #     df_cust_ids=df_cust_ids,
    #     df_cal=df_cal,
    #     df_prod_hier=df_prod_hier,
    #     sel_cols=sel_cols,
    #     st_date=st_date,
    #     end_date=prior_date,
    # )

    # df_trx = add_store_physical_store_location_no(df_trx)

    # df_trx = add_txn_loc(df_trx=df_trx, df_loc=df_loc)
    # df_trx = add_cogs(df_trx=df_trx, df_cogs=df_cogs)

    # if write:
    #     # saving final result
    #     write_blob(
    #         spark=spark,
    #         df=df_trx,
    #         path=path_preprocess_txn,
    #         file_format="delta",
    #         writemode="overwrite",
    #         partitionby="CALENDAR_DT",
    #     )

    # return df_trx
