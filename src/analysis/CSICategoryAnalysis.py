from datetime import datetime
import os

import altair as alt
from dotenv import load_dotenv
import polars as pl

from src.epicorAPI.CSIProducts import (
    get_csi_sales,
    categories,
    material_codes,
    competitors,
    CSIPart,
)

load_dotenv()


def to_json_grouped_by_month(df: pl.DataFrame) -> dict:
    result = {}
    for (year,), group in df.group_by("yearMonth"):
        columns = df.columns
        columns.remove("yearMonth")
        result[year] = group.select(columns).to_dicts()
    return result


def to_json_grouped_by_year(df: pl.DataFrame) -> dict:
    result = {}
    for (year,), group in df.group_by("year"):
        columns = df.columns
        columns.remove("year")
        result[year] = group.select(columns).to_dicts()
    return result


def to_json_grouped_by_category(df: pl.DataFrame) -> dict:
    result = {}
    for (year,), group in df.group_by("categoryid"):
        columns = df.columns
        columns.remove("categoryid")
        result[year] = group.select(columns).to_dicts()
    return result


def get_csi_products_df(df: pl.DataFrame):
    rows = df.filter(
        pl.col("partnum").str.starts_with("CSI-")
        | pl.col("partnum").str.split("-").list.get(0).is_in(categories)
    )[["changedate", "partnum", "unitprice"]]

    return pl.DataFrame(
        [
            CSIPart(row["changedate"], row["partnum"], row["unitprice"]).__dict__
            for row in rows.iter_rows(named=True)
        ]
    )


def get_csi_category_monthly_counts(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .with_columns(pl.col("changedate").dt.strftime("%Y-%m").alias("yearMonth"))
        .group_by(["yearMonth", "categoryid"])
        .agg(pl.col("cost").count().alias("totalCount"))
        .with_columns(
            (
                pl.col("totalCount")
                / pl.col("totalCount").sum().over("yearMonth")
                * 100
            ).alias("percentage")
        )
        .sort(["yearMonth", "categoryid"])
        .select("yearMonth", "categoryid", "totalCount", "percentage")
    )


def get_csi_category_monthly_revenues(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .with_columns(pl.col("changedate").dt.strftime("%Y-%m").alias("yearMonth"))
        .group_by(["yearMonth", "categoryid"])
        .agg(pl.col("cost").sum().alias("totalRevenue"))
        .with_columns(
            (
                pl.col("totalRevenue")
                / pl.col("totalRevenue").sum().over("yearMonth")
                * 100
            ).alias("percentage")
        )
        .sort(["yearMonth", "categoryid"])
        .select("yearMonth", "categoryid", "totalRevenue", "percentage")
    )


def get_csi_category_yearly_counts(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .with_columns(pl.col("changedate").dt.strftime("%Y").alias("year"))
        .group_by(["year", "categoryid"])
        .agg(pl.col("cost").count().alias("totalCount"))
        .with_columns(
            (
                pl.col("totalCount") / pl.col("totalCount").sum().over("year") * 100
            ).alias("percentage")
        )
        .sort(["year", "categoryid"])
        .select("year", "categoryid", "totalCount", "percentage")
    )


def get_csi_category_yearly_revenues(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .with_columns(pl.col("changedate").dt.strftime("%Y").alias("year"))
        .group_by(["year", "categoryid"])
        .agg(pl.col("cost").sum().alias("totalRevenue"))
        .with_columns(
            (
                pl.col("totalRevenue") / pl.col("totalRevenue").sum().over("year") * 100
            ).alias("percentage")
        )
        .sort(["year", "categoryid"])
        .select("year", "categoryid", "totalRevenue", "percentage")
    )


def get_csi_category_total_counts(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .group_by(["categoryid"])
        .agg(pl.col("cost").count().alias("totalCount"))
        .with_columns(
            (pl.col("totalCount") / pl.col("totalCount").sum() * 100).alias(
                "percentage"
            )
        )
        .sort(["categoryid"])
        .select("categoryid", "totalCount", "percentage")
    )


def get_csi_category_total_revenues(df: pl.DataFrame):
    return (
        df.with_columns(pl.col("changedate").str.strptime(pl.Datetime, strict=False))
        .group_by(["categoryid"])
        .agg(pl.col("cost").sum().alias("totalRevenue"))
        .with_columns(
            (pl.col("totalRevenue") / pl.col("totalRevenue").sum() * 100).alias(
                "percentage"
            )
        )
        .sort(["categoryid"])
        .select("categoryid", "totalRevenue", "percentage")
    )


def get_mics_totals(df: pl.DataFrame):
    return (
        df.filter(
            (pl.col("partnum").str.to_lowercase().str.contains("ship"))
            | (pl.col("partnum").str.to_lowercase().str.contains("sys"))
            | (pl.col("partnum").str.to_lowercase().str.contains("hdw"))
        )
        .with_columns(
            [
                pl.when(pl.col("partnum").str.to_lowercase().eq("x1ship"))
                .then(pl.lit("x1ship"))
                .when(pl.col("partnum").str.to_lowercase().eq("x1shipcsi"))
                .then(pl.lit("x1shipcsi"))
                .when(
                    (~pl.col("partnum").str.to_lowercase().eq("x1ship"))
                    & (~pl.col("partnum").str.to_lowercase().eq("x1shipcsi"))
                    & (pl.col("partnum").str.to_lowercase().str.contains("ship"))
                )
                .then(pl.lit("shipmisc"))
                .when(pl.col("partnum").str.to_lowercase().str.contains("sys"))
                .then(pl.lit("sys"))
                .when(pl.col("partnum").str.to_lowercase().str.contains("hdw"))
                .then(pl.lit("hdw"))
                .alias("category")
            ]
        )
        .group_by("category")
        .agg([pl.len().alias("count"), pl.sum("unitprice").alias("revenue")])
    )


def csi_category_overview_analysis(df: pl.DataFrame):
    csi_products_df = get_csi_products_df(df=df).filter(
        pl.col("categoryid").is_in(
            [
                "BFL",
                "CLD",
                "GRD",
                "SUR",
                "DVD",
            ]
        )
    )

    result = {"overview": {}}

    result["overview"]["misc"] = get_mics_totals(df=df).to_dicts()

    result["overview"]["categoryTotalCounts"] = to_json_grouped_by_category(
        get_csi_category_total_counts(csi_products_df)
    )
    result["overview"]["categoryTotalRevenues"] = to_json_grouped_by_category(
        get_csi_category_total_revenues(csi_products_df)
    )

    result["overview"]["categoryYearlyCounts"] = to_json_grouped_by_year(
        get_csi_category_yearly_counts(csi_products_df)
    )
    result["overview"]["categoryYearlyRevenues"] = to_json_grouped_by_year(
        get_csi_category_yearly_revenues(csi_products_df)
    )

    csi_category_montly_counts = get_csi_category_monthly_counts(csi_products_df)
    csi_category_montly_revenue = get_csi_category_monthly_revenues(csi_products_df)

    result["overview"]["categoryMonthlyCountsMonth"] = to_json_grouped_by_month(
        csi_category_montly_counts
    )
    result["overview"]["categoryMonthlyRevenuesMonth"] = to_json_grouped_by_month(
        csi_category_montly_revenue
    )
    result["overview"]["categoryMonthlyCountsCategory"] = to_json_grouped_by_category(
        csi_category_montly_counts
    )
    result["overview"]["categoryMonthlyRevenuesCategory"] = to_json_grouped_by_category(
        csi_category_montly_revenue
    )

    return result
