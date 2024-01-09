import logging
from typing import Optional

from cltv.utils.configuration import context
from cltv.utils.session import spark
from cltv.utils.decorators import timeit
from pyspark.sql import DataFrame as SparkDataFrame
from cltv.utils.utils_blob import backup_on_blob
from persoflow.tasks.product_insights.product_hier import (
    agg_trx_item_sk,
    clean_fam_hier,
    clean_prod_loc,
    get_dd_hier,
)

log = logging.getLogger(__name__)


@timeit
def task_create_product_hier(write: Optional[bool] = True) -> SparkDataFrame:
    """
    Creates a unique mapping between
    ITEM_SK -> ITEM_NO -> MASTER_ARTICLE -> LVLX_ID
    In case we find one lower level belonging to more than one parent, we only keep the
    one that has higher number of baskets in the last 3 months.
    Returns
    -------
    hier mapping
    """

    # a shortcut
    ctx = context

    # obtain required config  parameters
    region = ctx.region
    prior_date = ctx.prior_date
    n_weeks = ctx.config["hier_dedup_n_weeks"]

    df_prod = ctx.data.read("product")
    df_price_family = ctx.data.read("price_family")
    df_loc = ctx.data.read("location")
    df_trx = ctx.data.read("txnitem")

    # LOCATION ----------------------------------------------------------------
    log.info("Adding region to product table")
    df_prod = clean_prod_loc(df_prod, df_loc, region)
    n_prods = df_prod.select("ITEM_SK").distinct().count()

    # MASTER_ARTICLE ----------------------------------------------------------
    log.info("Adding master article to product table")
    df_prod = clean_fam_hier(df_prod, df_price_family)

    # TRX ---------------------------------------------------------------------
    log.info("Adding transactions to product table")
    df_prod = agg_trx_item_sk(df_prod, df_trx, prior_date, n_weeks)
    df_prod = backup_on_blob(spark, df_prod, ctx.data.path("prod_hier_dedup"))

    # DEDUP -------------------------------------------------------------------
    log.info("Cleaning product table")
    df_prod = get_dd_hier(df_prod=df_prod)
    msg = "Product table generating duplicates or missing rows"
    assert df_prod.count() == n_prods, log.error(msg)

    if write:
        ctx.data.write(
            dataset_id="prod_hier_dedup",
            df=df_prod,
            partition_by=None,
            break_on_writing=False,
            allow_empty=False,
        )

    return df_prod
