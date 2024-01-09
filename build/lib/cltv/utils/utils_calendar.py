from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window

from cltv.utils.utils_blob import backup_on_blob
from cltv.utils.session import spark



def adweek_generate(
    df_cal: SparkDataFrame, region: str = "quebec", fix_dt: str = "2021-04-29"
) -> SparkDataFrame:
    """
    Function that creates proper year_wk and year_wk_prev identifier based on fiscal_week_no and ad_week_no
    Parameters
    ----------

    df_cal:
        calendar dataframe imported
    region:
        filter the region we are computing
    fix_dt:
        date from which to impute missing year,week,quarter,period
        e.g dates on and beyond "2021-04-29" will have those fields imputed

    Returns
    -------
    A dataframe with proper year_wk and year_wk_prev identifier based on fiscal_week_no and ad_week_no
    """

    df_cal = (
        df_cal.select(
            "CALENDAR_DT",
            "AD_WEEK_NO",
            "AD_WEEK_END_DT",
            "REGION_DESC",
            "FISCAL_YEAR_NO",
            "CAL_YEAR_NO",
            "CAL_MONTH_NO",
            "CAL_QTR_NO",
            "DAY_OF_WEEK_NM",
            "FISCAL_QTR_NO",
            "FISCAL_PER",
        )
        .where(F.lower(F.col("REGION_DESC")) == region)
        .withColumn("CALENDAR_DT", F.regexp_replace("CALENDAR_DT", "D", ""))
        .withColumn("year", F.year(F.col("FISCAL_YEAR_NO")))
        .withColumn("week", F.lpad(F.col("AD_WEEK_NO"), 2, "0"))
        .withColumn("YEAR_WK", F.concat(F.col("year"), F.col("week")))
    )

    # TODO: determine a formula to be used to generate this as per Sreekanth's comment here: https://dev.azure.com/SobeysInc/Inno-Personalization/_git/Inno-Personalization/pullrequest/9861?_a=files&discussionId=34014&path=%2Fpersoflow%2Ftasks%2Fdata_preprocessing%2Fadweek_generation.py
    # Temporary fix to the ad_dates - the calendar table is wrong
    bad_dates_mapping = {
        "202801": ("2027-04-29", None),
        "202701": ("2026-04-30", "2026-05-06"),
        "202601": ("2025-05-01", "2025-05-06"),
        "202501": ("2024-05-02", "2024-05-08"),
        "202401": ("2023-05-04", "2023-05-10"),
        "202301": ("2022-05-05", "2022-05-11"),
        "202201": ("2021-04-29", "2021-05-05"),
        "202101": ("2020-04-30", "2020-05-06"),
        "202001": ("2019-05-02", "2019-05-08"),
        "201901": ("2018-05-03", "2018-05-09"),
        "201801": ("2017-05-04", "2017-05-10"),
    }

    # 1) Fix Bad Dates
    col_date = F.col("CALENDAR_DT")
    col_year_wk = F
    for year_wk, custom_period in bad_dates_mapping.items():
        st = custom_period[0]
        en = custom_period[1]
        col_year_wk = col_year_wk.when(col_date.between(st, en), F.lit(year_wk))

    col_year_wk = col_year_wk.otherwise(F.col("YEAR_WK"))

    df_cal = (
        df_cal.withColumn("YEAR_WK", col_year_wk)
        .withColumn(
            "week",
            F.when(F.col("CALENDAR_DT") >= fix_dt, F.lit(None)).otherwise(
                F.col("week")
            ),
        )
        .withColumn(
            "year",
            F.when(F.col("CALENDAR_DT") >= fix_dt, F.lit(None)).otherwise(
                F.col("year")
            ),
        )
        .withColumn(
            "quarter",
            F.when(F.col("CALENDAR_DT") >= fix_dt, F.lit(None)).otherwise(
                F.col("FISCAL_QTR_NO")
            ),
        )
        .withColumn(
            "period",
            F.when(F.col("CALENDAR_DT") >= fix_dt, F.lit(None)).otherwise(
                F.col("FISCAL_PER")
            ),
        )
        .withColumn(
            "period_int",
            F.when(F.col("CALENDAR_DT") >= fix_dt, F.lit(None)).otherwise(
                F.substring(F.col("FISCAL_PER"), 6, 2).cast("int")
            ),
        )
    )

    new_cal_df = df_cal.select(
        "CALENDAR_DT",
        "YEAR_WK",
        "year",
        "week",
        "CAL_YEAR_NO",
        "CAL_MONTH_NO",
        "CAL_QTR_NO",
        "DAY_OF_WEEK_NM",
        "quarter",
        "period",
        "period_int",
    ).dropDuplicates()

    new_cal_df = backup_on_blob(spark, new_cal_df)

    # 2) Fix Missing Dates After "2021-04-29"
    for yrwk, dates in bad_dates_mapping.items():
        if yrwk == "202801":
            end_bad_dt = dates[0]
            continue

        yr = yrwk[0:4]
        st_bad_dt = dates[0]

        impute_df = df_cal.filter(
            (F.col("CALENDAR_DT") >= fix_dt)
            & (F.col("CALENDAR_DT") >= st_bad_dt)
            & (F.col("CALENDAR_DT") < end_bad_dt)
        ).withColumn("year", F.lit(yr))

        impute_df = (
            impute_df.select(
                "CALENDAR_DT",
                "year",
                "CAL_YEAR_NO",
                "CAL_MONTH_NO",
                "CAL_QTR_NO",
                "DAY_OF_WEEK_NM",
                F.row_number()
                .over(Window.partitionBy("year").orderBy("CALENDAR_DT"))
                .alias("rn"),
            )
            .withColumn("week", F.lpad((F.floor((F.col("rn") - 1) / 7) + 1), 2, "0"))
            # a quarter is every 13 weeks, there can be a max of 4 quarters
            .withColumn("quarter_tmp", F.floor((F.col("rn") - 1) / (13 * 7)) + 1)
            .withColumn(
                "quarter",
                F.when(F.col("quarter_tmp") > 4, 4).otherwise(F.col("quarter_tmp")),
            )
            .drop("quarter_tmp")
            # a period is every 4 weeks, there can be a max of 13 periods
            .withColumn("period_int", F.floor((F.col("rn") - 1) / (4 * 7)) + 1)
            .withColumn(
                "period_tmp",
                F.when(F.col("period_int") > 13, 13).otherwise(F.col("period_int")),
            )
            .withColumn(
                "period",
                F.concat(
                    F.col("year"), F.lit("P"), F.lpad(F.col("period_tmp"), 2, "0")
                ),
            )
            .drop("period_tmp")
            .withColumn("YEAR_WK", F.concat(F.col("year"), F.col("week")))
        )

        new_cal_df = (
            new_cal_df.alias("a")
            .join(impute_df.alias("b"), on="CALENDAR_DT", how="outer")
            .select(
                "CALENDAR_DT",
                F.coalesce("a.YEAR_WK", "b.YEAR_WK").alias("YEAR_WK"),
                F.coalesce("a.year", "b.year").alias("year"),
                F.coalesce("a.week", "b.week").alias("week"),
                F.coalesce("a.CAL_YEAR_NO", "b.CAL_YEAR_NO").alias("CAL_YEAR_NO"),
                F.coalesce("a.CAL_MONTH_NO", "b.CAL_MONTH_NO").alias("CAL_MONTH_NO"),
                F.coalesce("a.CAL_QTR_NO", "b.CAL_QTR_NO").alias("CAL_QTR_NO"),
                F.coalesce("a.DAY_OF_WEEK_NM", "b.DAY_OF_WEEK_NM").alias(
                    "DAY_OF_WEEK_NM"
                ),
                F.coalesce("a.quarter", "b.quarter").alias("QUARTER"),
                F.coalesce("a.period", "b.period").alias("PERIOD"),
            )
        )
        # used in next loop
        end_bad_dt = dates[0]

    new_cal_df = backup_on_blob(spark, new_cal_df)

    ad_week_start_date = new_cal_df.groupBy("YEAR_WK").agg(
        F.min(F.col("CALENDAR_DT")).alias("ad_week_start_date")
    )

    new_cal_df = new_cal_df.join(ad_week_start_date, "YEAR_WK")

    cal_grouped = (
        new_cal_df.groupBy("YEAR_WK")
        .agg(F.max("CALENDAR_DT").alias("CALENDAR_DT"))
        .withColumn(
            "YEAR_WK_PREV", F.lag("YEAR_WK").over(Window.orderBy("CALENDAR_DT"))
        )
    )

    result_df = new_cal_df.join(
        cal_grouped.select("YEAR_WK", "YEAR_WK_PREV"), on="YEAR_WK", how="left"
    ).withColumn("year", F.col("year").cast("int"))

    return result_df
