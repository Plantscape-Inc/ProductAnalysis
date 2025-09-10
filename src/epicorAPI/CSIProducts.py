from datetime import datetime
import json
import os
import re

import altair as alt
from dotenv import load_dotenv
import pymssql
import polars as pl
import pandas as pd

from src.epicorAPI.Orders import sqlexec_local

load_dotenv()

categories = ['SUR',
              'LED',
              'DVD',
              'HDW',
              'BIO',
              'LBR',
              'BFL',
              'CLD',
              'SYS',
              'DSGNSER',
              'GRD']

subcategories = {
    "BFL": [
        "SGL",
        "CRV",
        "STK",
        "FLD",
        "SGP",
        "FCT",
        "DRP",
    ],
    "CLD": [
        "CON",
        "WFL",
        "WSL",
        "WFD",
        "PFL",
        "PFP",
        "FRM",
        "STK",
        "FCT",
        "NCH",
        "BLD",
        "EXP",
        "LAT",
        "FAB",
        "FMD",
    ],
    "GRD": [
        "FLD",
        "FPG",
        "SEC",
        "BLD",
        "WFL",
        "TLE",
        "PFL",
        "PLG",
        "NCH",
    ],
    "SUR": [
        "BLD",
        "PLG",
        "FPG",
        "PFL",
        "PNL",
        "PFP",
        "PNP",
        "FLD",
        "TXD",
        "STK",
        "OLP",
        "LNE",
        "CRV",
        "CRK",
        "CRL",
        "COV",
        "EXD",
        "ECH",
    ],
    "DVD": [
        "SGL",
        "STK",
        "CRV",
        "DRP",
        "SEC",
        "DRS",
    ]
}

material_codes = ["PSH", "SND", "SWT", "SWT2"]

competitors = {
    "3F": "3form",
    "9W": "9Wood",
    "AA": "Acoustical Art Concepts",
    "AE": "Acoustical Surfaces",
    "AF": "Acoufelt",
    "AI": "ASI Architectural",
    "AL": "aLight",
    "ALW": "ALW",
    "AP": "Acoustical Products & Systems",
    "AR": "Armstrong",
    "AS": "Altispace",
    "AU": "Autex",
    "AX": "Axis",
    "AW": "ALW",
    "AZ": "Artizin",
    "BA": "Baux",
    "BC": "Beta Calco",
    "BS": "BuzziSpace",
    "CA": "Carnegie",
    "CO": "Coronet",
    "CR": "C.W. Wood Craft",
    "CW": "Coowin",
    "CT": "CertainTeed",
    "DA": "DecorAcoustics",
    "DF": "DFB",
    "DZ": "DFB",  # Note: if this was a typo, remove or fix
    "EP": "Ecophon",
    "ES": "eScape",
    "EZ": "EzoBord",
    "FE": "Felt Right",
    "FI": "FI Interiors",
    "FF": "FilzFelt",
    "FL": "Finelite",
    "FP": "Focal Point",
    "FR": "Frasch",
    "FS": "FSorb",
    "FT": "Fact Design",
    "GA": "G&S Acoustics",  # Original GS, corrected
    "GS": "G&S Acoustics",
    "HD": "Hunter Douglas",
    "HS": "Hush",
    "HU": "Huddl Design",
    "IA": "Impact Acoustics",
    "IN": "Inhabit",
    "JS": "J2 Systems",
    "KN": "Kinetics",
    "KR": "Kirei",
    "LA": "LightArt",
    "LV": "Lamvin",
    "LW": "LoftWall",
    "MA": "MPS Acoustics",
    "MB": "Mobern",
    "ML": "Metalumen",
    "MX": "Maxxit",
    "MU": "Muffle",
    "OC": "OCL",
    "PS": "PolySorb",
    "RA": "Ritz Acoustics",
    "RU": "Rulon International",
    "SI": "Sabin",
    "SL": "Saylite",
    "SK": "Skutchi",
    "SB": "Soelberg",
    "SN": "Sonus",
    "SS": "Sound Seal",
    "SP": "SoundPly",
    "TF": "Turf",
    "TT": "Trend Office Technology",
    "US": "USG",
    "UV": "Unika Vaev",
    "WG": "Wolf Gordon",
    "WO": "Woven Image",
    "ZI": "Zintra"
}


class CSIPart(object):
    def __init__(self, date: datetime, part_str: str, cost: float):
        self.custom = False if part_str.find("CUST") == -1 else True
        self.part_str = part_str
        self.changedate = date
        self.cost = cost

        part = part_str.split("-")

        if part[0] == "CSI":
            part.remove("CSI")

        self.category_id = part[0]

        if part[1] == "CUST":
            self.custom = True
            return

        self.subcategory = part[1]

        if len(part) > 2:
            if part[2].find("CUST") == -1:
                self.product_id = part[2]
        else:
            return

        temp_list = part[3:]

        for index in range(len(temp_list) - 1):
            match = re.match(r"^\d*X\d*$", temp_list[index])
            if match:
                self.dimensions = match.group(0)
                temp_list.remove(temp_list[index])


def fetch_orderdtl():
    order_dtl = sqlexec_local("orderdtl", f"""SELECT *
                   FROM orderdtl
    """)

    return order_dtl.with_columns(
        pl.col("requestdate").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
        pl.col("changedate").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
        pl.col("partnum").str.replace(" ", "")
    )


def get_csi_products():
    return (
        fetch_orderdtl()
        .filter(
            (pl.col("partnum").str.starts_with("CSI-"))
            | (pl.col("partnum").str.split("-").list.get(0).is_in(categories))
        )
        .with_columns(
            pl.when(pl.col("partnum").str.starts_with("CSI-"))
            .then(pl.col("partnum").str.slice(4))  # remove "CSI-"
            .otherwise(pl.col("partnum"))
            .alias("partnum")
        )
        .select(["changedate", "ordernum", "partnum", "unitprice", "linedesc"])
    )


if __name__ == "__main__":
    csi_products = get_csi_products()
    print(csi_products)
