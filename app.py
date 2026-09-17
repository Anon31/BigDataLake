import os
import hashlib
import hmac
import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, quote, parse_qsl

import pandas as pd
import altair as alt

import requests
import streamlit as st
import duckdb

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def sign_request(method, url, headers, access_key, secret_key, region, payload_hash=EMPTY_SHA256):
    now = datetime.datetime.utcnow()
    datestamp = now.strftime("%Y%m%d")
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path or "/"

    canonical_uri = quote(path, safe="/-_.~")
    query_pairs = sorted(parse_qsl(parsed.query, keep_blank_values=True))
    canonical_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in query_pairs
    )

    headers["x-amz-date"] = amz_date
    headers["x-amz-content-sha256"] = payload_hash
    headers["host"] = host

    signed_headers_list = sorted(headers.keys())
    signed_headers = ";".join(signed_headers_list)
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in signed_headers_list)

    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
    credential_scope = f"{datestamp}/{region}/s3/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n" + hashlib.sha256(
        canonical_request.encode()
    ).hexdigest()

    def sign(key, msg):
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k_date = sign(("AWS4" + secret_key).encode(), datestamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, "s3")
    k_signing = sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    headers["Authorization"] = auth_header
    return headers


def list_parquet_files():
    url = f"{S3_ENDPOINT}/{S3_BUCKET}?list-type=2&prefix=&delimiter=/"
    headers = {}
    headers = sign_request("GET", url, headers, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION)

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    files = []
    for content in root.findall(".//s3:Contents", ns):
        key = content.find("s3:Key", ns).text
        if key.endswith(".parquet"):
            files.append(key)

    for prefix_el in root.findall(".//s3:CommonPrefixes", ns):
        sub_prefix = prefix_el.find("s3:Prefix", ns).text
        sub_url = f"{S3_ENDPOINT}/{S3_BUCKET}?list-type=2&prefix={sub_prefix}"
        sub_headers = {}
        sub_headers = sign_request("GET", sub_url, sub_headers, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION)
        sub_resp = requests.get(sub_url, headers=sub_headers, timeout=10)
        sub_resp.raise_for_status()
        sub_root = ET.fromstring(sub_resp.text)
        for content in sub_root.findall(".//s3:Contents", ns):
            key = content.find("s3:Key", ns).text
            if key.endswith(".parquet"):
                files.append(key)

    return sorted(files)


def init_duckdb():
    con = duckdb.connect()
    con.sql(f"""
        CREATE SECRET (
            TYPE s3,
            KEY_ID '{S3_ACCESS_KEY}',
            SECRET '{S3_SECRET_KEY}',
            ENDPOINT 'minio:9000',
            REGION '{S3_REGION}',
            USE_SSL false,
            URL_STYLE 'path'
        );
    """)
    return con


st.set_page_config(page_title="Big Data Lake - Parquet Viewer", layout="wide")
st.title("Big Data Lake - Parquet Viewer")

st.sidebar.header("Configuration")
st.sidebar.write(f"**S3 Endpoint:** {S3_ENDPOINT}")
st.sidebar.caption("Endpoint interne du réseau Docker (nom du service `minio`).")
st.sidebar.write(f"**Bucket:** {S3_BUCKET}")

try:
    files = list_parquet_files()
except Exception as e:
    st.error(f"Erreur lors de la connexion a MinIO: {e}")
    st.stop()

if not files:
    st.warning("Aucun fichier .parquet trouve dans le bucket.")
    st.stop()

st.success(f"{len(files)} fichier(s) parquet trouve(s)")

selected_files = st.multiselect(
    "Selectionner les fichiers a charger:",
    files,
    default=files[:min(5, len(files))],
)

if not selected_files:
    st.warning("Selectionnez au moins un fichier.")
    st.stop()

s3_paths = [f"s3://{S3_BUCKET}/{f}" for f in selected_files]

st.subheader("Apercu des donnees")

try:
    con = init_duckdb()
    path_list = ", ".join(f"'{p}'" for p in s3_paths)
    df = con.sql(
        f"SELECT * FROM read_parquet([{path_list}], union_by_name=true, filename=true)"
    ).df()
    con.close()
except Exception as e:
    st.error(f"Erreur DuckDB lors de la lecture des fichiers: {e}")
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Lignes", len(df))
col2.metric("Colonnes", len(df.columns))
col3.metric("Fichiers charges", len(selected_files))

st.subheader("Graphiques")

def chart_or_skip(label, chart):
    if chart is not None:
        with st.expander(label, expanded=True):
            st.altair_chart(chart, use_container_width=True)

def base_df():
    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
    return d

d = base_df()
now = pd.Timestamp.now(tz="UTC")
d = d[d["timestamp"].notna()]

# 1. Activite temporelle (evenements par heure)
if not d.empty:
    hourly = (
        d["timestamp"].dt.floor("h").value_counts()
        .sort_index().rename_axis("heure").reset_index(name="count")
    )
    chart = (
        alt.Chart(hourly)
        .mark_area(point=True)
        .encode(
            x=alt.X("heure:T", title="Temps"),
            y=alt.Y("count:Q", title="Evenements"),
            tooltip=[alt.Tooltip("heure:T", title="Heure"), alt.Tooltip("count:Q", title="Evenements")],
        )
    )
    chart_or_skip("Activite temporelle", chart)

# 2. Repartition par niveau de log
if "level" in d.columns:
    levels = d["level"].fillna("inconnu").value_counts().reset_index()
    levels.columns = ["level", "count"]
    chart = (
        alt.Chart(levels)
        .mark_bar(cornerRadiusTopLeft=3)
        .encode(
            x=alt.X("level:N", title="Niveau"),
            y=alt.Y("count:Q", title="Nombre"),
            color=alt.Color("level:N", legend=None),
            tooltip=["level:N", "count:Q"],
        )
    )
    chart_or_skip("Repartition par niveau de log", chart)

# 3. Tokens par session (entree / sortie)
tok_cols = {"entree": "tokens.input", "sortie": "tokens.output"}
if all(c in d.columns for c in tok_cols.values()):
    tod = d.copy()
    for c in tok_cols.values():
        tod[c] = pd.to_numeric(tod[c], errors="coerce").fillna(0)
    tokens = tod.groupby("session.id")[list(tok_cols.values())].sum().reset_index()
    tokens_long = tokens.melt(
        id_vars="session.id", var_name="type", value_name="tokens"
    ).fillna(0)
    chart = (
        alt.Chart(tokens_long)
        .mark_bar()
        .encode(
            x=alt.X("session.id:N", title="Session", axis=alt.Axis(labels=False)),
            y=alt.Y("tokens:Q", title="Tokens"),
            color=alt.Color("type:N", title="Type de token"),
            tooltip=["session.id:N", "type:N", "tokens:Q"],
        )
    )
    chart_or_skip("Tokens par session", chart)

# 4. Top sessions (nombre de messages)
if "session.id" in d.columns:
    top = (
        d["session.id"].fillna("inconnu").value_counts()
        .head(10).rename_axis("session").reset_index(name="count")
    )
    chart = (
        alt.Chart(top)
        .mark_bar(cornerRadiusTopLeft=3)
        .encode(
            x=alt.X("count:Q", title="Messages"),
            y=alt.Y("session:N", sort="-x", title="Session"),
            color=alt.Color("count:Q", legend=None),
            tooltip=["session:N", "count:Q"],
        )
    )
    chart_or_skip("Top 10 des sessions", chart)

# 5. Cout cumule
if "cost" in d.columns:
    cost = d.copy()
    cost["cost"] = pd.to_numeric(cost["cost"], errors="coerce")
    cost = cost[["timestamp", "cost"]].dropna().sort_values("timestamp")
    cost["cummule"] = cost["cost"].cumsum()
    if not cost.empty:
        chart = (
            alt.Chart(cost)
            .mark_line(point=True)
            .encode(
                x=alt.X("timestamp:T", title="Temps"),
                y=alt.Y("cummule:Q", title="Cout cumule ($)"),
                tooltip=[alt.Tooltip("timestamp:T", title="Temps"), alt.Tooltip("cummule:Q", title="Cout cumule")],
            )
        )
        chart_or_skip("Cout cumule", chart)

st.dataframe(df, use_container_width=True)

with st.expander("Schema des colonnes"):
    st.write(df.dtypes)

with st.expander("Statistiques"):
    st.write(df.describe())
