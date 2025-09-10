import functools
import os

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
import pymssql

from epicorAPI.Orders import fetch_order, fetch_order_local

load_dotenv()

app = Flask(__name__)
JWT_SECRET = "plantscapeinc"

CORS(app, supports_credentials=True, origins=["http://localhost:5173"])

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



if __name__ == "__main__":
    app.run(port=5002, debug=True)
