import functools
import os

from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
import pymssql
import polars as pl

from epicorAPI.CSIProductAnalysis import category_revenue, get_total_revenue, material_revenue, competitor_summary
from epicorAPI.CSIProducts import get_competitor_sales, get_csi_sales

load_dotenv()

app = Flask(__name__)
JWT_SECRET = "plantscapeinc"

CORS(app, supports_credentials=True, origins=["*"])

authed_users = ["ben.barcaskey@plantscapeinc.com"]


def authenticate(**options):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization", None)
            if not auth_header or not auth_header.startswith("Bearer "):
                return jsonify({"error": "missing token"}), 401

            token = auth_header.split(" ")[1]
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                if payload['sub'] not in authed_users:
                    return jsonify({"error": "unauthorized"}), 412

                return func(*args, **kwargs)
            except jwt.ExpiredSignatureError:
                return jsonify({"error": "token expired"}), 401
            except jwt.InvalidTokenError:
                return jsonify({"error": "invalid token"}), 401

        return wrapper

    return decorator


conn = pymssql.connect(
    server=os.getenv("EPICOR_SERVER"),
    user=os.getenv("EPICOR_USER"),
    password=os.getenv("EPICOR_PASSWORD"),
    database=os.getenv("EPICOR_DATABASE"),
    port=os.getenv("EPICOR_PORT"),
    tds_version=str(os.getenv("EPICOR_TDS_VERSION"))
)

@app.route("/")
def index():
    return {"healthCheck": True}


@app.route("/categoryRevenue", methods=["GET"])
def category_revenue_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True if request.args.get("local") else False

    if local:
        csi_sales = get_csi_sales(startdate=start_date, enddate=end_date)
    else:
        csi_sales = get_csi_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(csi_sales)
    category_revenue_df = category_revenue(csi_sales, total_revenue)

    return jsonify(category_revenue_df.to_dicts())


@app.route("/materialRevenue", methods=["GET"])
def material_revenue_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True if request.args.get("local") else False

    if local:
        csi_sales = get_csi_sales(startdate=start_date, enddate=end_date)
    else:
        csi_sales = get_csi_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(csi_sales)

    material_revenue_df = material_revenue(csi_sales, total_revenue)

    return jsonify(material_revenue_df.to_dicts())


@app.route("/competitorSales", methods=["GET"])
def competitor_sales_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True if request.args.get("local") else False

    local = True

    if local:
        comp_sales = get_competitor_sales(startdate=start_date, enddate=end_date)
    else:
        comp_sales = get_competitor_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(comp_sales)
    comp_category_revenue_df = (competitor_summary(comp_sales, total_revenue)
                                .filter(pl.col("totalRevenue") > 0)
                                .sort("percentage", descending=True))

    return jsonify(comp_category_revenue_df.to_dicts())


if __name__ == "__main__":
    app.run(port=5002, debug=True)
