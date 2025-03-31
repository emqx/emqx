from flask import Flask, abort
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

app = Flask(__name__)

@app.route("/query/<namespace>/<table>")
def query(namespace, table):
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": "http://iceberg-rest:8181",
                "warehouse": "s3a://warehouse/wh",
            }
        )
        tab = catalog.load_table(f"{namespace}.{table}")
        rows = tab.scan().to_pandas()
        return rows.to_json(orient="records")
    except NoSuchTableError:
        return f"table {namespace}.{table} not found!\n", 404
