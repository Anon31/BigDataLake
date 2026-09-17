import argparse
import re
import subprocess
import sys
import pandas as pd
from pathlib import Path

LOG_PATH = Path.home() / ".local/share/opencode/log/opencode.log"
PARQUET_PATH = "/tmp/opencode-logs.parquet"

BUCKET = "my-bucket"

def parse_log_line(line):
    pattern = r'(\w[\w.]*)=(?:"([^"]*)"|(\S*))'
    return {k: (v2 if v2 else v3) for k, v2, v3 in re.findall(pattern, line)}

def main():
    parser = argparse.ArgumentParser(description="Exporte les logs opencode en Parquet vers MinIO S3")
    parser.add_argument("--session", help="sessionID : exporte uniquement les traces de cette conversation")
    args = parser.parse_args()

    if args.session:
        key = f"sessions/{args.session}.parquet"
    else:
        key = "opencode-logs.parquet"

    rows = []
    for line in LOG_PATH.read_text().splitlines():
        if line.strip():
            entry = parse_log_line(line)
            if args.session and entry.get("session.id") != args.session:
                continue
            rows.append(entry)

    if not rows:
        print(f"Aucune trace pour la session '{args.session}', aucun export effectué.", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    df.to_parquet(PARQUET_PATH, index=False)
    print(f"Parquet écrit : {PARQUET_PATH} ({len(df)} lignes)")

    subprocess.run([
        "docker", "cp", PARQUET_PATH, f"minio-s3:{PARQUET_PATH}"
    ], check=True)
    subprocess.run([
        "docker", "exec", "minio-s3", "mc", "cp",
        PARQUET_PATH, f"local/{BUCKET}/{key}"
    ], check=True)
    print(f"Uploadé vers s3://{BUCKET}/{key}")

    head = pd.read_parquet(PARQUET_PATH)
    print("\nAperçu :")
    print(head.head())

if __name__ == "__main__":
    main()
