import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import pandas as pd
from pathlib import Path

DB_PATH = Path.home() / ".local/share/opencode/opencode.db"
PARQUET_PARTS_PATH = "/tmp/opencode-parts.parquet"
PARQUET_SESSIONS_PATH = "/tmp/opencode-sessions.parquet"

BUCKET = "my-bucket"
KEY_PARTS = "conversations/parts.parquet"
KEY_SESSIONS = "conversations/sessions.parquet"


def connect_ro():
    tmp_dir = tempfile.mkdtemp(prefix="opencode-db-")
    tmp_db = Path(tmp_dir) / "opencode.db"
    shutil.copy2(DB_PATH, tmp_db)
    for suffix in ("-wal", "-shm"):
        src = Path(str(DB_PATH) + suffix)
        if src.exists():
            shutil.copy2(src, Path(str(tmp_db) + suffix))
    try:
        con = sqlite3.connect(tmp_db)
        con.execute("PRAGMA busy_timeout = 10000")
        return con
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def load_sessions(con):
    cols = ["id", "parent_id", "slug", "directory", "path", "title",
            "version", "summary_additions", "summary_deletions", "time_created", "time_updated"]
    rows = con.execute(f"SELECT {', '.join(cols)} FROM session").fetchall()
    return pd.DataFrame(rows, columns=cols)


def load_parts(con, session_id=None):
    base = (
        "SELECT p.id, p.message_id, p.session_id, p.time_created, p.time_updated, "
        "p.data, m.data, s.title "
        "FROM part p "
        "JOIN message m ON m.id = p.message_id "
        "JOIN session s ON s.id = p.session_id "
    )
    params = ()
    if session_id:
        base += "WHERE p.session_id = ?"
        params = (session_id,)

    rows = con.execute(base, params).fetchall()

    records = []
    for part_id, message_id, session_id_, time_created, time_updated, part_data, message_data, title in rows:
        p = json.loads(part_data)
        m = json.loads(message_data)
        record = {
            "part_id": part_id,
            "message_id": message_id,
            "session_id": session_id_,
            "time_created": time_created,
            "time_updated": time_updated,
            "role": m.get("role"),
            "title": title,
        }
        ptype = p.get("type")
        record["part_type"] = ptype

        if ptype == "text":
            record["text"] = p.get("text")
        elif ptype == "reasoning":
            record["text"] = p.get("text")
            record["reasoning_time"] = json.dumps(p.get("time"), default=str)
        elif ptype == "tool":
            state = p.get("state", {})
            record["tool"] = p.get("tool")
            record["tool_status"] = state.get("status")
            record["tool_input"] = json.dumps(state.get("input"), default=str, ensure_ascii=False)
            record["tool_output"] = state.get("output")
        elif ptype == "step-finish":
            record["reason"] = p.get("reason")
            tokens = p.get("tokens") or {}
            record["tokens_input"] = tokens.get("input")
            record["tokens_output"] = tokens.get("output")
            record["tokens_total"] = tokens.get("total")
            record["cost"] = p.get("cost")
        elif ptype == "patch":
            record["files"] = json.dumps(p.get("files", []), default=str, ensure_ascii=False)
        records.append(record)

    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(
        description="Exporte les conversations opencode (message/part) en Parquet vers MinIO S3")
    parser.add_argument("--session", help="sessionID : exporte uniquement cette session")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Base opencode introuvable : {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    con = connect_ro()
    try:
        if args.session:
            parts = load_parts(con, session_id=args.session)
            print(f"Export session '{args.session}' : {len(parts)} parts")
            if parts.empty:
                print("Aucune part pour cette session, pas d'export conversation.", file=sys.stderr)
                sys.exit(1)
            parts.to_parquet(PARQUET_PARTS_PATH, index=False)
            key_parts = f"sessions/{args.session}_parts.parquet"
            subprocess.run(["docker", "cp", PARQUET_PARTS_PATH, f"minio-s3:{PARQUET_PARTS_PATH}"], check=True)
            subprocess.run(["docker", "exec", "minio-s3", "mc", "cp", PARQUET_PARTS_PATH,
                            f"local/{BUCKET}/{key_parts}"], check=True)
            print(f"Uploadé vers s3://{BUCKET}/{key_parts}")
            return

        sessions = load_sessions(con)
        parts = load_parts(con)
        print(f"{len(sessions)} sessions, {len(parts)} parts")

        parts.to_parquet(PARQUET_PARTS_PATH, index=False)
        sessions.to_parquet(PARQUET_SESSIONS_PATH, index=False)

        for local, key in [
            (PARQUET_PARTS_PATH, KEY_PARTS),
            (PARQUET_SESSIONS_PATH, KEY_SESSIONS),
        ]:
            subprocess.run(["docker", "cp", local, f"minio-s3:{local}"], check=True)
            subprocess.run(["docker", "exec", "minio-s3", "mc", "cp", local,
                            f"local/{BUCKET}/{key}"], check=True)
            print(f"Uploadé vers s3://{BUCKET}/{key}")

        print("\nApercu parts :")
        print(parts[["time_created", "role", "part_type", "tool", "text", "tool_input", "tool_output"]].head(5).to_string())
        print("\nApercu sessions :")
        print(sessions.head(5).to_string())
    finally:
        con.close()


if __name__ == "__main__":
    main()