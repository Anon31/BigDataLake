import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "my-bucket")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")


def wait_for_nessie(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{NESSIE_URI}/config", timeout=5) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode())
                    print(f"[iceberg-init] Nessie prêt sur {NESSIE_URI} "
                          f"(default branch: {body.get('defaultBranch', 'main')})")
                    return True
        except Exception as exc:  # noqa: BLE001
            print(f"[iceberg-init] Nessie pas encore prêt ({exc})")
        time.sleep(5)
    raise SystemExit(f"[iceberg-init] Nessie injoignable après {timeout}s sur {NESSIE_URI}")


def build_spark():
    return (
        SparkSession.builder
        .appName("iceberg-init")
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


def create_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.opencode")
    for table, view, source in [
        ("parts", "src_parts", f"s3a://{BUCKET}/conversations/parts.parquet"),
        ("sessions", "src_sessions", f"s3a://{BUCKET}/conversations/sessions.parquet"),
    ]:
        spark.read.parquet(source).createOrReplaceTempView(view)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS iceberg.opencode.{table} "
            f"USING iceberg AS SELECT * FROM {view}"
        )
        count = spark.sql(f"SELECT count(*) AS n FROM iceberg.opencode.{table}").first()["n"]
        print(f"[iceberg-init] table iceberg.opencode.{table} prête : {count} lignes")


def main():
    wait_for_nessie()
    spark = build_spark()
    try:
        create_tables(spark)
        print("\n=== Tables Iceberg (catalogue iceberg) ===")
        spark.sql("SHOW TABLES IN iceberg.opencode").show(truncate=False)
        print("=== Snapshots (parts) ===")
        spark.sql("SELECT snapshot_id, committed_at, operation, summary "
                  "FROM iceberg.opencode.parts.system.snapshots").show(truncate=False)
        print("[iceberg-init] terminé ✔")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()