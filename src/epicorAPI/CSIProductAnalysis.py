from datetime import datetime
import json
import os
import re

import altair as alt
from dotenv import load_dotenv
import pymssql
import polars as pl

from src.epicorAPI.Orders import sqlexec, sqlexec_local
from src.epicorAPI.CSIProducts import categories, material_codes, competitors
from src.epicorAPI.utils import print_price


def get_total_revenue(df: pl.DataFrame) -> float:
    return df['unitprice'].sum()


def get_category_revenue_sum(df: pl.DataFrame, category_id: str):
    return (df.filter(pl.col('partnum').str.contains(category_id)))['unitprice'].sum()


def category_revenue(df: pl.DataFrame, total_rev: float):
    category_revenue_list = []
    for cat in categories:
        rev = get_category_revenue_sum(df, cat)
        category_revenue_list.append({
            "category": cat,
            "price": f"{rev:,.2f}",
            "percentage": float(f"{(rev / total_rev) * 100:.2f}"),
            "revenue": float(f"{rev:.2f}"),
        })

    return pl.DataFrame(category_revenue_list)


def get_material_revenue(df: pl.DataFrame, material_code: str):
    return (df.filter(pl.col('partnum').str.contains(material_code)))['unitprice'].sum()


def material_revenue(df: pl.DataFrame, total_rev: float) -> pl.DataFrame:
    material_revenue_list = []
    for mat in material_codes:
        rev = get_material_revenue(df, mat)
        material_revenue_list.append({
            "material": mat,
            "price": print_price(rev),
            "percentage": float(f"{(rev / total_rev) * 100:.2f}"),
            "revenue": float(f"{rev:.2f}"),
        })

    return pl.DataFrame(material_revenue_list)


def competitor_summary(df: pl.DataFrame, total_comp_revenue: float):
    competitor_summary_list = []
    for comp in competitors:
        competitor_orders = df.filter(
            pl.any_horizontal(
                pl.col("partnum")
                .str.split_exact("-", 2)
                .struct[2].str.starts_with(comp)
            )
        )
        competitor_summary_list.append({
            "competitor": competitors[comp],
            "competitorId": comp,
            "quoteCount": len(competitor_orders),
            "totalRevenue": float(competitor_orders['unitprice'].sum()),
            "totalRevenuePrice": print_price(float(competitor_orders['unitprice'].sum())),
            "percentage": float(f"{(competitor_orders['unitprice'].sum() / total_comp_revenue) * 100:.2f}"),
        })

    return pl.DataFrame(competitor_summary_list)
