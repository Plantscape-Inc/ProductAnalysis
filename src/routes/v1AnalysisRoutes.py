from datetime import datetime
from flask import request, jsonify, Blueprint
import polars as pl

from epicorAPI.CSIProductAnalysis import category_revenue, get_total_revenue, material_revenue, competitor_summary
from epicorAPI.CSIProducts import get_competitor_sales, get_csi_sales

v1_routes = Blueprint("v1", __name__, url_prefix="/v1")


@v1_routes.route("/categoryRevenue", methods=["GET"])
def category_revenue_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True #if request.args.get("local") else False

    # print(conn.cursor())
    if local:
        csi_sales = get_csi_sales(startdate=start_date, enddate=end_date)
    else:
        pass
        # csi_sales = get_csi_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(csi_sales)
    category_revenue_df = category_revenue(csi_sales, total_revenue)

    return jsonify(category_revenue_df.to_dicts())


@v1_routes.route("/materialRevenue", methods=["GET"])
def material_revenue_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True# if request.args.get("local") else False

    # print(conn.cursor())

    if local:
        csi_sales = get_csi_sales(startdate=start_date, enddate=end_date)
    else:
        pass
        # csi_sales = get_csi_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(csi_sales)

    material_revenue_df = material_revenue(csi_sales, total_revenue)

    return jsonify(material_revenue_df.to_dicts())


@v1_routes.route("/competitorSales", methods=["GET"])
def competitor_sales_route():
    start_date = datetime.fromisoformat(request.args.get("startdate")) if "startdate" in list(
        request.args.keys()) else None
    end_date = datetime.fromisoformat(request.args.get("enddate")) if "enddate" in list(request.args.keys()) else None
    local = True# if request.args.get("local") else False


    if local:
        comp_sales = get_competitor_sales(startdate=start_date, enddate=end_date)
    else:
        pass
        # comp_sales = get_competitor_sales(conn.cursor(), startdate=start_date, enddate=end_date)

    total_revenue = get_total_revenue(comp_sales)
    comp_category_revenue_df = (competitor_summary(comp_sales, total_revenue)
                                .filter(pl.col("totalRevenue") > 0)
                                .sort("percentage", descending=True))

    return jsonify(comp_category_revenue_df.to_dicts())
