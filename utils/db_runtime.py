from __future__ import annotations

import os
from typing import Any

import pandas as pd
from sqlalchemy import Engine, create_engine, text


def create_engine_from_url(database_url: str, *, pool_recycle: int | None = 280) -> Engine:
    """
    Create a SQLAlchemy engine suitable for long-running web apps.

    PythonAnywhere can close idle MySQL connections after ~300s; their docs recommend
    setting pool_recycle to a value below that (e.g. 280 seconds).
    """
    kw: dict[str, Any] = {"future": True}
    if pool_recycle is not None:
        kw["pool_recycle"] = int(pool_recycle)
    return create_engine(database_url, **kw)


def get_engine_from_env(
    *,
    env_var: str = "DATABASE_URL",
    pool_recycle: int | None = 280,
) -> Engine:
    database_url = os.environ.get(env_var)
    if not database_url:
        raise RuntimeError(f"Missing required environment variable: {env_var}")
    return create_engine_from_url(database_url, pool_recycle=pool_recycle)


def get_testnames(engine: Engine) -> list[str]:
    q = "SELECT DISTINCT TESTNAME FROM analysis WHERE TESTNAME IS NOT NULL ORDER BY TESTNAME"
    df = pd.read_sql_query(q, engine)
    if df.empty or "TESTNAME" not in df.columns:
        return []
    return df["TESTNAME"].dropna().astype(str).tolist()


def get_projects_df(engine: Engine) -> pd.DataFrame:
    return pd.read_sql_query("SELECT PROJECTID, PI_NAME, PI_INSTITUTION FROM projects", engine)


def get_projects_lookup(engine: Engine) -> dict[str, dict[str, str]]:
    df = get_projects_df(engine)
    if df.empty:
        return {}
    df = df.dropna(subset=["PROJECTID"])
    if df.empty:
        return {}
    df["PROJECTID"] = df["PROJECTID"].astype(str)
    return df.set_index("PROJECTID")[["PI_NAME", "PI_INSTITUTION"]].fillna("").to_dict(orient="index")


def get_project_rundates_lookup(engine: Engine) -> dict[str, dict[str, Any]]:
    q = """
    SELECT PROJECTID, MIN(RUNDATE) AS min_date, MAX(RUNDATE) AS max_date
    FROM analysis
    WHERE PROJECTID IS NOT NULL AND RUNDATE IS NOT NULL
    GROUP BY PROJECTID
    """
    df = pd.read_sql_query(q, engine)
    if df.empty:
        return {}
    df = df.dropna(subset=["PROJECTID"])
    if df.empty:
        return {}
    df["PROJECTID"] = df["PROJECTID"].astype(str)
    return df.set_index("PROJECTID")[["min_date", "max_date"]].to_dict(orient="index")


def get_project_rundates_for_project(engine: Engine, *, project_id: str) -> dict[str, Any] | None:
    q = """
    SELECT MIN(RUNDATE) AS min_date, MAX(RUNDATE) AS max_date
    FROM analysis
    WHERE PROJECTID = :project_id AND RUNDATE IS NOT NULL
    """
    df = pd.read_sql_query(text(q), engine, params={"project_id": project_id})
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    if row.get("min_date") is None or row.get("max_date") is None:
        return None
    return row


def fetch_analysis_subset(
    engine: Engine,
    *,
    testname: str,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    project_id: str | None = None,
    units_val: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    cols = columns or [
        "TESTNAME",
        "TESTVALUE",
        "COHORT",
        "HEURISTIC",
        "GBA",
        "UNITS",
        "RUNDATE",
        "SEX",
        "PATNO",
        "PROJECTID",
        "AGE_AT_VISIT",
        "CLINICAL_EVENT",
    ]
    select_cols = ", ".join([c for c in cols])

    where_parts: list[str] = ["TESTNAME = :testname"]
    params: dict[str, Any] = {"testname": str(testname)}

    selected = cohort_filter or []
    if selected:
        placeholders = []
        for i, val in enumerate(selected):
            key = f"cohort_{i}"
            placeholders.append(f":{key}")
            params[key] = str(val)
        where_parts.append(f"COHORT IN ({', '.join(placeholders)})")
    else:
        where_parts.append("1 = 0")

    if gba_filter_mode == "excluded":
        # Keep this portable across MySQL/SQLite by casting to a generic numeric type.
        where_parts.append("(GBA IS NULL OR CAST(GBA AS DECIMAL(10,4)) != 1.0)")

    if project_id is not None:
        where_parts.append("PROJECTID = :project_id")
        params["project_id"] = str(project_id)

    if units_val is not None:
        where_parts.append("UNITS = :units_val")
        params["units_val"] = str(units_val)

    sql = f"SELECT {select_cols} FROM analysis WHERE {' AND '.join(where_parts)}"
    out = pd.read_sql_query(text(sql), engine, params=params)
    return out

