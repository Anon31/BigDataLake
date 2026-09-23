import base64
import datetime
import decimal
import json
import os
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql import SparkSession

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
HOST = os.environ.get("SPARK_SQL_HOST", "0.0.0.0")
PORT = int(os.environ.get("SPARK_SQL_PORT", "9100"))
MAX_ROWS = int(os.environ.get("SPARK_SQL_MAX_ROWS", "2000"))


def wait_for_nessie(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{NESSIE_URI}/config", timeout=5) as resp:
                if resp.status == 200:
                    print(f"[spark-sql] Nessie prêt sur {NESSIE_URI}")
                    return True
        except Exception as exc:  # noqa: BLE001
            print(f"[spark-sql] Nessie pas encore prêt ({exc})")
        time.sleep(5)
    raise SystemExit(f"[spark-sql] Nessie injoignable après {timeout}s sur {NESSIE_URI}")


def build_spark():
    return (
        SparkSession.builder
        .appName("spark-sql-server")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.iceberg.uri", NESSIE_URI)
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def to_jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    return str(value)


def execute_sql(sql):
    t0 = time.time()
    df = spark.sql(sql)
    columns = list(df.columns)
    rows = [list(r) for r in df.limit(MAX_ROWS).collect()]
    affected = df.count()
    return {
        "ok": True,
        "columns": columns,
        "rows": rows,
        "count": len(rows),
        "rows_affected": affected,
        "elapsed_ms": int((time.time() - t0) * 1000),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # noqa: A003
        print(f"[spark-sql] {self.address_string()} {fmt % args}")

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            tables = []
            try:
                rows = spark.sql("SHOW TABLES IN iceberg.opencode").collect()
                tables = [r["tableName"] for r in rows]
            except Exception:  # noqa: BLE001
                tables = []
            self._send_json(200, {"status": "ok", "tables": tables})
        else:
            self._send_json(404, {"ok": False, "error": "route inconnue"})

    def do_POST(self):
        if self.path != "/query":
            self._send_json(404, {"ok": False, "error": "route inconnue"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            sql = str(body.get("sql", "")).strip().rstrip(";").strip()
            if not sql:
                raise ValueError("requete SQL vide")
            self._send_json(200, execute_sql(sql))
        except Exception as exc:  # noqa: BLE001
            self._send_json(400, {"ok": False, "error": str(exc)})


if __name__ == "__main__":
    wait_for_nessie()
    spark = build_spark()
    print("[spark-sql] SparkSession prête")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"[spark-sql] API SQL Spark démarée sur http://{HOST}:{PORT}")
    server.serve_forever()