import functools
import os

from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
import pymssql

from src.analysis.OverviewAnalysis import overview_stats
from src.epicorAPI.utils import sqlexec, sqlexec_local

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


@app.route("/")
def index():
    return {"healthCheck": True}


@app.route("/brandsOverview", methods=["GET"])
def brands_overview_route():
    startdate = datetime.fromisoformat(request.args.get("startDate")) if "startDate" in list(
        request.args.keys()) else datetime(2022, 1, 1)
    enddate = datetime.fromisoformat(request.args.get("endDate")) if "endDate" in list(
        request.args.keys()) else datetime.today().date()
    local = True  # if "local" in list(request.args.keys()) and str(request.args["local"]).lower() in ['true', '1', 'yes'] else False

    include_raw = False if "includeRaw" in list(request.args.keys()) and str(
        request.args["includeRaw"]).lower() in ['false', '0', 'no'] else True

    if local:
        order_dtl = sqlexec_local("orderdtl", f"""SELECT *
                       FROM orderdtl
                      WHERE changedate BETWEEN '{startdate}' and '{enddate}'
        """)
    else:
        conn = pymssql.connect(
            server=os.getenv("EPICOR_SERVER"),
            user=os.getenv("EPICOR_USER"),
            password=os.getenv("EPICOR_PASSWORD"),
            database=os.getenv("EPICOR_DATABASE"),
            port=os.getenv("EPICOR_PORT"),
            tds_version=str(os.getenv("EPICOR_TDS_VERSION"))
        )
        order_dtl = sqlexec(conn.cursor(), f"""SELECT *
                       FROM orderdtl
                      WHERE changedate BETWEEN '{startdate}' and '{enddate}'
        """)

    return jsonify(overview_stats(order_dtl, include_raw))


if __name__ == "__main__":
    app.run(port=5002, debug=True)
