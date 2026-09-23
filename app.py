import os
import json
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
SPARK_SQL_URL = os.getenv("SPARK_SQL_URL", "http://spark-sql-server:9100")

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


def parse_tool_input(raw):
    if not raw:
        return ""
    try:
        return json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
    except Exception:
        return str(raw)


def spark_health():
    try:
        resp = requests.get(f"{SPARK_SQL_URL}/health", timeout=10)
        return resp.status_code == 200, resp.json()
    except Exception as e:
        return False, str(e)


def run_spark_query(sql):
    resp = requests.post(f"{SPARK_SQL_URL}/query", json={"sql": sql}, timeout=180)
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error", "Erreur inconnue du serveur Spark SQL"))
    return payload


st.set_page_config(page_title="Big Data Lake - Agent Visualizer", layout="wide")
st.title("Big Data Lake - Agent Visualizer")

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

conv_files = [f for f in files if f.startswith("conversations/")]
log_files = [f for f in files if not f.startswith("conversations/")]

try:
    con = init_duckdb()
except Exception as e:
    st.error(f"Erreur DuckDB: {e}")
    st.stop()

has_conv = False
parts_df = pd.DataFrame()
sessions_df = pd.DataFrame()
if len(conv_files) >= 2:
    try:
        parts_df = con.sql(
            f"SELECT * FROM read_parquet('s3://{S3_BUCKET}/conversations/parts.parquet')"
        ).df()
        sessions_df = con.sql(
            f"SELECT * FROM read_parquet('s3://{S3_BUCKET}/conversations/sessions.parquet')"
        ).df()
        has_conv = True
    except Exception as e:
        st.sidebar.warning(f"Conversations non chargees: {e}")

if has_conv:
    parts_df["dt"] = pd.to_datetime(parts_df["time_created"], unit="ms", errors="coerce", utc=True)
    sessions_df["dt"] = pd.to_datetime(sessions_df["time_created"], unit="ms", errors="coerce", utc=True)
    con.register("parts", parts_df)
    con.register("sessions", sessions_df)

tab_labels = ["Vue d'ensemble"]
if has_conv:
    tab_labels += ["Timeline", "Agent actions", "Fichiers modifies", "Sessions", "SQL explorer"]
else:
    tab_labels += ["Conversations (export manquant)"]
tab_labels += ["Spark SQL / Iceberg"]

tab_count = len(tab_labels)
tabs = st.tabs(tab_labels)

# ----------------------------------------------------------------------
# Onglet 1 : vue d'ensemble (logs + graphiques)
# ----------------------------------------------------------------------
with tabs[0]:
    selected_files = st.multiselect(
        "Selectionner les fichiers de logs a charger:",
        log_files,
        default=log_files[:min(5, len(log_files))],
        key="log_files",
    )

    if not selected_files:
        st.info("Selectionnez au moins un fichier de log pour afficher le dashboard.")
    else:
        s3_paths = [f"s3://{S3_BUCKET}/{f}" for f in selected_files]

        try:
            path_list = ", ".join(f"'{p}'" for p in s3_paths)
            df = con.sql(
                f"SELECT * FROM read_parquet([{path_list}], union_by_name=true, filename=true)"
            ).df()
            con.register("logs", df)
        except Exception as e:
            st.error(f"Erreur DuckDB lors de la lecture des fichiers: {e}")
            df = pd.DataFrame()

        if not df.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Lignes", len(df))
            col2.metric("Colonnes", len(df.columns))
            col3.metric("Fichiers charges", len(selected_files))

            st.subheader("Graphiques")

            def chart_or_skip(label, chart):
                if chart is not None:
                    with st.expander(label, expanded=True):
                        st.altair_chart(chart, width="stretch")

            d = df.copy()
            d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
            d = d[d["timestamp"].notna()]

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
                chart_or_skip("Top 10 des sessions (logs)", chart)

            if "cost" in d.columns:
                cost = d.copy()
                cost["cost"] = pd.to_numeric(cost["cost"], errors="coerce")
                cost = cost[["timestamp", "cost"]].dropna().sort_values("timestamp")
                if not cost.empty:
                    cost["cummule"] = cost["cost"].cumsum()
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

            st.subheader("Donnees")
            st.dataframe(df, width="stretch")

            with st.expander("Schema des colonnes"):
                st.write(df.dtypes)

            with st.expander("Statistiques"):
                st.write(df.describe())

# ----------------------------------------------------------------------
# Onglet 2 : Timeline conversation
# ----------------------------------------------------------------------
if has_conv:
    with tabs[1]:
        st.subheader("Timeline de la conversation")

        sessions_list = sessions_df.sort_values("time_created", ascending=False)
        session_ids = sessions_list["id"].tolist()
        session_labels = [f"{r['title'][:60]} ({r['id']})" for _, r in sessions_list.iterrows()]
        sel = st.selectbox(
            "Session:", list(zip(session_labels, session_ids)), index=0,
            format_func=lambda x: x[0], key="dl_timeline"
        )
        sel_id = sel[1]

        sub = parts_df[parts_df["session_id"] == sel_id].sort_values("time_created")

        if sub.empty:
            st.info("Aucune part pour cette session.")
        else:
            max_events = st.slider("Nombre max d'evenements affiches:",
                                   min_value=10, max_value=len(sub), value=min(200, len(sub)),
                                   key="sl_timeline")
            sub = sub.iloc[:max_events]

            for _, r in sub.iterrows():
                pt = r.get("part_type")
                if pt == "text" and r.get("role") == "user":
                    with st.chat_message("user"):
                        st.write(r.get("text") or "")
                elif pt == "text":
                    with st.chat_message("assistant"):
                        st.write(r.get("text") or "")
                elif pt == "reasoning":
                    with st.chat_message("assistant"):
                        st.caption(f"🧠 Raisonnement: {(r.get('text') or '')[:2000]}")
                elif pt == "tool":
                    tool = r.get("tool") or "?"
                    status = r.get("tool_status") or ""
                    with st.chat_message("assistant"):
                        with st.expander(f"🔧 {tool} · {status}", expanded=False):
                            c1, c2 = st.columns(2)
                            c1.markdown("**Input**")
                            c1.code(parse_tool_input(r.get("tool_input")), language="json")
                            c2.markdown("**Output**")
                            c2.text(str(r.get("tool_output") or "")[:4000])
                elif pt == "step-finish":
                    with st.chat_message("assistant"):
                        st.caption(f"⬆ Step termine · {r.get('tokens_total') or '-'} tokens · {r.get('cost') or '-'}")
                elif pt == "patch":
                    with st.chat_message("assistant"):
                        try:
                            patch_files = json.loads(r.get("files") or "[]")
                        except (TypeError, json.JSONDecodeError):
                            patch_files = []
                        st.caption("📝 Fichiers: " + ", ".join(patch_files if isinstance(patch_files, list) else [str(patch_files)]))
                elif pt == "step-start":
                    with st.chat_message("assistant"):
                        st.caption("⬇ Debut de step")

    # ----------------------------------------------------------------------
    # Onglet 3 : Agent actions (tool calls)
    # ----------------------------------------------------------------------
    with tabs[2]:
        st.subheader("Actions de l'agent (appels d'outils)")

        tools_df = parts_df[parts_df["part_type"] == "tool"].copy()
        if tools_df.empty:
            st.info("Aucun appel d'outil enregistre.")
        else:
            tool_names = sorted(tools_df["tool"].dropna().unique().tolist())
            sel_tool = st.multiselect("Outils:", tool_names, default=tool_names, key="ml_tools")

            sel_session_ids = st.multiselect(
                "Sessions:",
                sessions_df.sort_values("time_created", ascending=False)["id"].tolist(),
                default=[],
            )

            filtered = tools_df
            if not sel_session_ids:
                sel_session_ids = tools_df["session_id"].unique().tolist()
            filtered = filtered[filtered["session_id"].isin(sel_session_ids)]
            if sel_tool:
                filtered = filtered[filtered["tool"].isin(sel_tool)]

            st.metric("Appels d'outils", len(filtered))

            shown = filtered.sort_values("time_created")
            for _, r in shown.iterrows():
                tool = r.get("tool") or "?"
                title = r.get("title") or ""
                with st.expander(f"🔧 {tool} · {title[:50]} · {r.get('tool_status') or ''}", expanded=False):
                    c1, c2 = st.columns(2)
                    c1.markdown("**Input**")
                    c1.code(parse_tool_input(r.get("tool_input")), language="json")
                    c2.markdown("**Output**")
                    c2.text(str(r.get("tool_output") or "")[:4000])

    # ----------------------------------------------------------------------
    # Onglet 4 : Fichiers modifies (patches)
    # ----------------------------------------------------------------------
    with tabs[3]:
        st.subheader("Fichiers modifies par l'agent")

        patches_df = parts_df[parts_df["part_type"] == "patch"].copy()
        if patches_df.empty:
            st.info("Aucun patch enregistre.")
        else:
            file_records = []
            for _, r in patches_df.iterrows():
                try:
                    files = json.loads(r.get("files") or "[]")
                except Exception:
                    files = []
                for f in files:
                    file_records.append({
                        "session_id": r.get("session_id"),
                        "title": r.get("title"),
                        "time_created": r.get("dt"),
                        "file": f,
                    })
            if file_records:
                files_df = pd.DataFrame(file_records)
                st.metric("Fichiers au total", len(files_df))
                per_session = files_df.groupby("title").size().reset_index(name="count").sort_values("count", ascending=False)
                chart = (
                    alt.Chart(per_session)
                    .mark_bar(cornerRadiusTopLeft=3)
                    .encode(
                        x=alt.X("count:Q", title="Fichiers"),
                        y=alt.Y("title:N", sort="-x", title="Session"),
                        tooltip=["title:N", "count:Q"],
                    )
                )
                st.altair_chart(chart, width="stretch")
                st.dataframe(files_df[["time_created", "title", "file"]], width="stretch")

    # ----------------------------------------------------------------------
    # Onglet 5 : Sessions
    # ----------------------------------------------------------------------
    with tabs[4]:
        st.subheader("Sessions")

        if parts_df.empty:
            st.info("Aucune donnee.")
        else:
            agg = (
                parts_df.groupby("session_id")
                .agg(
                    nb_parts=("part_id", "count"),
                    nb_tools=("tool", lambda s: s.notna().sum()),
                    nb_messages=("message_id", "nunique"),
                    cout=("cost", lambda s: pd.to_numeric(s, errors="coerce").sum()),
                    tokens=("tokens_total", lambda s: pd.to_numeric(s, errors="coerce").sum()),
                )
                .reset_index()
            )
            merged = sessions_df[["id", "title", "dt", "summary_additions", "summary_deletions"]].merge(
                agg, left_on="id", right_on="session_id", how="left"
            )
            merged = merged.sort_values("dt", ascending=False)
            st.dataframe(merged, width="stretch")

            chart = (
                alt.Chart(merged.dropna(subset=["nb_parts"]))
                .mark_bar(cornerRadiusTopLeft=3)
                .encode(
                    x=alt.X("nb_parts:Q", title="Parts"),
                    y=alt.Y("title:N", sort="-x", title="Session"),
                    tooltip=["title:N", "nb_parts:Q", "nb_tools:Q", "cout:Q"],
                )
            )
            st.altair_chart(chart, width="stretch")

    # ----------------------------------------------------------------------
    # Onglet 6 : SQL explorer
    # ----------------------------------------------------------------------
    with tabs[5]:
        st.subheader("Explorateur SQL (DuckDB)")

        st.markdown(
            "Tables disponibles : `parts`, `sessions`, `logs`. Exemple : "
            "`SELECT session_id, part_type, count(*) FROM parts GROUP BY 1, 2 ORDER BY 3 DESC`"
        )
        sql = st.text_area(
            "Requete SQL:",
            value="SELECT session_id, part_type, count(*) AS n FROM parts GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50",
            height=120, key="sql_query"
        )
        if st.button("Executer", key="sql_run"):
            try:
                res = con.sql(sql).df()
                st.success(f"{len(res)} ligne(s)")
                st.dataframe(res, width="stretch")
            except Exception as e:
                st.error(f"Erreur SQL: {e}")
else:
    st.warning(
        "Les données de conversation ne sont pas disponibles. Lancez "
        "`python3.10 scripts/db_to_parquet.py` puis rafraichissez la page."
    )

# ----------------------------------------------------------------------
# Onglet : Spark SQL / Iceberg (cluster Spark + catalogue Nessie)
# ----------------------------------------------------------------------
with tabs[tab_count - 1]:
    st.subheader("Explorateur SQL Spark / Iceberg")

    status, info = spark_health()
    if status:
        st.success(f"Serveur Spark SQL joignable : {SPARK_SQL_URL}")
        tables = info.get("tables") or []
        if tables:
            st.caption("Tables Iceberg disponibles : " + ", ".join(f"`iceberg.opencode.{t}`" for t in tables))
        else:
            st.caption(
                "Catalogue `iceberg`, base `opencode`, tables `parts`/`sessions`. "
                "Aucune table trouvee : lancez `docker compose run --rm iceberg-init`."
            )
    else:
        st.error(f"Serveur Spark SQL injoignable ({SPARK_SQL_URL}) : {info}")
        st.caption("Lancez `docker compose up -d spark-sql-server` - le premier demarrage prend ~30s.")

    st.markdown(
        "Exemples :\n"
        "- `SELECT * FROM iceberg.opencode.parts LIMIT 20`\n"
        "- `SELECT part_type, count(*) AS n FROM iceberg.opencode.parts GROUP BY 1 ORDER BY 2 DESC`\n"
        "- `SELECT * FROM iceberg.opencode.parts.system.snapshots`\n"
        "- `SHOW TABLES IN iceberg.opencode`"
    )
    spark_sql = st.text_area(
        "Requete SQL (Spark / Iceberg):",
        value="SELECT * FROM iceberg.opencode.parts LIMIT 20",
        height=120, key="spark_sql_query"
    )
    if st.button("Executer sur Spark", key="spark_sql_run"):
        with st.spinner("Spark execute la requete..."):
            try:
                res = run_spark_query(spark_sql)
                c1, c2 = st.columns(2)
                c1.metric("Lignes renvoyees", res["count"])
                c2.metric("Duree", f"{res['elapsed_ms'] / 1000:.1f} s")
                if res["rows"]:
                    st.dataframe(pd.DataFrame(res["rows"], columns=res["columns"]), width="stretch")
                else:
                    st.success(f"Requete executee ({res['rows_affected']} ligne(s) affectee(s), aucune ligne renvoyee).")
            except Exception as e:
                st.error(f"Erreur Spark SQL: {e}")