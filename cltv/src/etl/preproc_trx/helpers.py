from typing import Optional, List

from pyspark.sql import Window, dataframe as SparkDataFrame
from pyspark.sql import functions as F
from inno_utils.loggers import log



def add_customer_card_id(
    df: SparkDataFrame, df_cust_reachable: SparkDataFrame
) -> SparkDataFrame:
    """
    Add customer_id_card to any transaction type dataframe that contains customer_card_sk.

    Parameters
    ----------
    df: DataFrame
        Any DataFrame with a CUSTOMER_CARD_SK column

    df_cust_reachable:
        DataFrame that maps CUSTOMER_CARD_SK to CUSTOMER_CARD_ID

    Returns
    -------
    DataFrame
        Original DataFrame with CUSTOMER_CARD_ID representing a unique customer
    """

    df_cust_ids = (
        df_cust_reachable.withColumn(
            "CUSTOMER_CARD_SK",
            F.substring("CUSTOMER_CARD_SK", 2, len("CUSTOMER_CARD_SK")),
        )
        .select("CUSTOMER_CARD_SK", "CUSTOMER_CARD_ID")
        .distinct()
    )
    count_of_active_cust = df_cust_ids.count()
    log.info(f"within start_date and end_date window, unique customer count = {count_of_active_cust} ")
    
    df = df.join(df_cust_ids, on="CUSTOMER_CARD_SK", how="left")
    df = df.where(~F.col("CUSTOMER_CARD_ID").isNull())
    
    return df

def clean_txn(
    df_txn: SparkDataFrame,
    df_cust_ids: SparkDataFrame,
    df_cal: SparkDataFrame,
    df_prod_hier: SparkDataFrame,
    sel_cols: Optional[List[str]] = [],
    st_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    Function responsible for preparing the transactional data in a standardized way. For
    that we need to remove unneeded customers, add new customer's id, and add hierarchy
    of the product.
    Parameters
    ----------
    df_txn: SparkDataFrame
        Raw transaction data
    df_cust_ids: SparkDataFrame
        Table with customer_sk and the customer_card_id
    df_cal: SparkDataFrame
        Calendar date to transform to a weekly level
    df_prod_hier: SparkDataFrame
        Product hierarchy cleaned of duplicates
    sel_cols: List
        Selection of columns we want to keep from trx data
    st_date:
        Starting date we want to filter down txn
    end_date:
        Ending date we want to filter down txn

    Returns
    -------
        df_txn: SparkDataFrame, Transaction data cleaned.
    """

    # 1) Select columns applied
    if len(sel_cols) > 0:
        df_txn = df_txn.select(*sel_cols)

    # 2) Filter dates if needed
    if st_date:
        df_txn = df_txn.filter(F.col("CALENDAR_DT") >= st_date)
    if end_date:
        df_txn = df_txn.filter(F.col("CALENDAR_DT") <= end_date)

    # 3) Remove customers
    cond = (F.col("CUSTOMER_SK") != 1) & (F.col("CUSTOMER_SK") != -1)
    df_txn = df_txn.where(cond)

    # 4) Add customer card id   
    df_txn = add_customer_card_id(df_txn, df_cust_ids)

    # 5) Add beginning of the week
    df_txn = df_txn.join(df_cal, "CALENDAR_DT")

    # 6) Add hierarchy of product. IMPORTANT: we want to include all sales regardless of them
    # having a full hierarchy in the product table.
    df_txn = df_txn.join(df_prod_hier, "ITEM_SK", "left")

    return df_txn


def add_txn_loc(df_trx: SparkDataFrame, df_loc: SparkDataFrame) -> SparkDataFrame:
    """
    Adding location information in the transaction data
    Parameters
    ----------
    df_trx: SparkDataFrame
        Raw transaction data
    df_loc: SparkDataFrame
        location information
    Returns
    -------
    df_trx: SparkDataFrame, transactions with locationg
    """
    w = Window.partitionBy(
        "STORE_PHYSICAL_LOCATION_NO"
    ).orderBy(F.desc("VALID_FROM_DTTM"))
    df_loc = (
        df_loc.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select(
            "STORE_PHYSICAL_LOCATION_NO",
            "NATIONAL_BANNER_DESC",
        )
    )
    df_trx = df_trx.join(df_loc, "STORE_PHYSICAL_LOCATION_NO", "left")
    df_trx = df_trx.na.drop(subset=["STORE_PHYSICAL_LOCATION_NO"])

    return df_trx


def add_cogs(df_trx: SparkDataFrame, df_cogs: SparkDataFrame) -> SparkDataFrame:
    """
    Function responsible of adding the E2E margin and net price in the transaction data.
    When we are reporting margin, we should always use E2E margin
    Parameters
    ----------
    df_trx: SparkDataFrame, data with transaction data
    df_cogs: SparkDataFrame, data with the process E2E margins

    Returns
    -------
    SparkDataFrame, data with the added columns of margin
    """

    lst_cogs_merge = ["YEAR_WK", "ITEM_NO", "NATIONAL_BANNER_DESC"]
    df_cogs = df_cogs.select(
        "YEAR_WK", "ITEM_NO", "NATIONAL_BANNER_DESC", "NET_PRICE", "E2E_MARGIN"
    ).dropDuplicates(subset=lst_cogs_merge)

    # Add into transactions
    df_trx = (
        df_trx.withColumn(
            "PRICE_UNITS",
            F.when(F.col("ITEM_UOM_CD") == "KG", "KG").otherwise("UNIT"),
        )
        .join(df_cogs, lst_cogs_merge, "left")
        .withColumn(
            "E2E_MARGIN",
            F.when(
                F.col("PRICE_UNITS") == "KG", F.col("E2E_MARGIN") * F.col("ITEM_WEIGHT")
            ).otherwise(F.col("E2E_MARGIN") * F.col("ITEM_QTY")),
        )
    )

    return df_trx

