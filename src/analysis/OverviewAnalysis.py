from datetime import datetime
import os

import altair as alt
from dotenv import load_dotenv
import polars as pl

from src.epicorAPI.CSIProducts import get_csi_sales, categories, material_codes, competitors

from src.epicorAPI.CSIProducts import fetch_orderdtl_local

load_dotenv()


def monthly_counts(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col("changedate").str.strptime(pl.Datetime, strict=False),
        )
        .with_columns(
            pl.col("changedate").dt.strftime("%Y-%m").alias("yearMonth")
        )
        .group_by("yearMonth")
        .agg(pl.len().alias("count"))
        .sort("yearMonth")
        .select("yearMonth", "count")
    )

def monthly_revenue_counts(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.col("changedate").str.strptime(pl.Datetime, strict=False),
        )
        .with_columns(
            pl.col("changedate").dt.strftime("%Y-%m").alias("yearMonth")
        )
        .group_by("yearMonth")
        .agg(pl.col("unitprice").sum().alias("totalRevenue"))
        .sort("yearMonth")
        .select("yearMonth", "totalRevenue")
    )


def overview_stats(df: pl.DataFrame, include_raw=False):
    result = {}
    csi_orders = df.filter(
        (pl.col("partnum").str.starts_with("CSI-"))
        | (pl.col("partnum").str.split("-").list.get(0).is_in(categories))
    ).with_columns(
        pl.when(pl.col("partnum").str.starts_with("CSI-"))
        .then(pl.col("partnum").str.slice(4))  # remove "CSI-"
        .otherwise(pl.col("partnum"))
        .alias("partnum")
    ).select(['changedate', 'ordernum', 'partnum', 'unitprice'])

    csilk_orders = df.filter(
        (~pl.col("partnum").str.starts_with("CSI-"))
        & ~(pl.col("partnum").str.split("-").list.get(0).is_in(categories))
    ).select(['changedate', 'ordernum', 'partnum', 'unitprice'])

    result['brandLineItemsCount'] = {
        "csi": len(csi_orders),
        "csilk": len(csilk_orders),
        "total": len(df)
    }

    result['brandOverallRevenue'] = {
        "csi": csi_orders['unitprice'].sum(),
        "csilk": csilk_orders['unitprice'].sum(),
        "total": df['unitprice'].sum()
    }

    result['monthlyCountsTimeseries'] = {
        "csi": monthly_counts(csi_orders).to_dicts(),
        "csilk": monthly_counts(csilk_orders).to_dicts(),
    }
    result['monthlyRevenueTimeseries'] = {
        "csi": monthly_revenue_counts(csi_orders).to_dicts(),
        "csilk": monthly_revenue_counts(csilk_orders).to_dicts(),
    }

    if include_raw:
        result['csiTimeseries'] = csi_orders.to_dicts()
        result['csilkTimeseries'] = csilk_orders.to_dicts()

    return result
