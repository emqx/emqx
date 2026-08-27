from databricks import sql
from flask import Flask, request
import logging

app = Flask(__name__)


@app.route("/sql", methods=["POST"])
def run_sql():
    req = request.get_json()
    hostname = req['hostname']
    path = req['path']
    token = req['token']
    req_sql = req['sql']
    with sql.connect(
            server_hostname = hostname,
            http_path = path,
            access_token = token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(req_sql)
            rows = cursor.fetchall()
            col_names = [d[0] for d in cursor.description]
            res = [
                dict(zip(col_names, row))
                for row in rows
            ]
            return res


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)
    app.run(host="0.0.0.0", port=8000, debug=True)
