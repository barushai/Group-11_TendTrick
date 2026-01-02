import pandas as pd
from databricks import sql
import streamlit as st

@st.cache_data(ttl=600, show_spinner=False)
def run_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    cfg = st.secrets.get("databricks", {})
    host = cfg.get("server_hostname")
    http_path = cfg.get("http_path")
    token = cfg.get("access_token")
    assert host and http_path and token, "Missing Databricks secrets in .streamlit/secrets.toml"
    if params:
        query = query.format(**params)
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame.from_records(rows, columns=cols)

def sql_list(values: list[str]) -> str:
    if not values:
        return "()"
    safe = [v.replace("'", "''") for v in values]
    return "(" + ",".join([f"'{v}'" for v in safe]) + ")"
