from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import Engine, create_engine, text


@dataclass(frozen=True)
class DbPaths:
    db_path: Path

    @property
    def sqlite_url(self) -> str:
        return f"sqlite:///{self.db_path.as_posix()}"


def default_db_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "processed" / "biomarkers.sqlite"


def get_engine(db_path: Path | str | None = None) -> Engine:
    path = Path(db_path) if db_path is not None else default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{path.as_posix()}", future=True)
    init_db(engine)
    return engine


def init_db(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    PROJECTID INTEGER PRIMARY KEY,
                    PI_NAME TEXT,
                    PI_INSTITUTION TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS analysis (
                    PATNO INTEGER,
                    SEX TEXT,
                    AGE_AT_VISIT REAL,
                    COHORT TEXT,
                    CLINICAL_EVENT TEXT,
                    TYPE TEXT,
                    TESTNAME TEXT,
                    TESTVALUE REAL,
                    UNITS TEXT,
                    RUNDATE TEXT,
                    PROJECTID INTEGER,
                    RV REAL,
                    GBA REAL,
                    PREDICTED REAL,
                    DRIVEN REAL,
                    HEURISTIC TEXT
                )
                """
            )
        )

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_analysis_testname ON analysis(TESTNAME)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_analysis_projectid ON analysis(PROJECTID)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_analysis_cohort ON analysis(COHORT)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_analysis_heuristic ON analysis(HEURISTIC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_analysis_rundate ON analysis(RUNDATE)"))

        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_analysis_dedupe
                ON analysis(PATNO, PROJECTID, TESTNAME, CLINICAL_EVENT, TYPE, RUNDATE)
                """
            )
        )


def _to_records(df: pd.DataFrame, *, columns: Iterable[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    sub = df.loc[:, [c for c in columns if c in df.columns]].copy()
    return sub.where(pd.notna(sub), None).to_dict("records")


def upsert_projects(engine: Engine, df: pd.DataFrame) -> None:
    if df.empty:
        return
    if "PROJECTID" not in df.columns:
        raise ValueError("projects upsert requires a PROJECTID column.")

    dfc = df.copy()
    dfc["PROJECTID"] = pd.to_numeric(dfc["PROJECTID"], errors="coerce")
    dfc = dfc.dropna(subset=["PROJECTID"])
    if dfc.empty:
        return
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(int)

    rows = _to_records(dfc, columns=["PROJECTID", "PI_NAME", "PI_INSTITUTION"])
    if not rows:
        return

    sql = text(
        """
        INSERT INTO projects (PROJECTID, PI_NAME, PI_INSTITUTION)
        VALUES (:PROJECTID, :PI_NAME, :PI_INSTITUTION)
        ON CONFLICT(PROJECTID) DO UPDATE SET
            PI_NAME=excluded.PI_NAME,
            PI_INSTITUTION=excluded.PI_INSTITUTION
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def append_analysis(engine: Engine, df: pd.DataFrame) -> None:
    if df.empty:
        return

    required = {
        "PATNO",
        "PROJECTID",
        "TESTNAME",
        "CLINICAL_EVENT",
        "TYPE",
        "RUNDATE",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"analysis append is missing required columns: {sorted(missing)}")

    dfc = df.copy()
    dfc["PATNO"] = pd.to_numeric(dfc["PATNO"], errors="coerce")
    dfc["PROJECTID"] = pd.to_numeric(dfc["PROJECTID"], errors="coerce")
    dfc = dfc.dropna(subset=["PATNO", "PROJECTID", "TESTNAME", "CLINICAL_EVENT", "TYPE", "RUNDATE"])
    if dfc.empty:
        return
    dfc["PATNO"] = dfc["PATNO"].astype(int)
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(int)

    # Store RUNDATE as ISO date string for easy MIN/MAX and portability.
    dfc["RUNDATE"] = pd.to_datetime(dfc["RUNDATE"], errors="coerce").dt.date.astype("string")
    dfc = dfc.dropna(subset=["RUNDATE"])
    if dfc.empty:
        return

    cols = [
        "PATNO",
        "SEX",
        "AGE_AT_VISIT",
        "COHORT",
        "CLINICAL_EVENT",
        "TYPE",
        "TESTNAME",
        "TESTVALUE",
        "UNITS",
        "RUNDATE",
        "PROJECTID",
        "RV",
        "GBA",
        "PREDICTED",
        "DRIVEN",
        "HEURISTIC",
    ]
    rows = _to_records(dfc, columns=cols)
    if not rows:
        return

    insert_sql = text(
        """
        INSERT OR IGNORE INTO analysis (
            PATNO, SEX, AGE_AT_VISIT, COHORT, CLINICAL_EVENT, TYPE, TESTNAME, TESTVALUE,
            UNITS, RUNDATE, PROJECTID, RV, GBA, PREDICTED, DRIVEN, HEURISTIC
        )
        VALUES (
            :PATNO, :SEX, :AGE_AT_VISIT, :COHORT, :CLINICAL_EVENT, :TYPE, :TESTNAME, :TESTVALUE,
            :UNITS, :RUNDATE, :PROJECTID, :RV, :GBA, :PREDICTED, :DRIVEN, :HEURISTIC
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)


def get_testnames(engine: Engine) -> list[str]:
    q = "SELECT DISTINCT TESTNAME FROM analysis WHERE TESTNAME IS NOT NULL ORDER BY TESTNAME"
    df = pd.read_sql_query(q, engine)
    if df.empty or "TESTNAME" not in df.columns:
        return []
    return df["TESTNAME"].dropna().astype(str).tolist()


def get_projects_df(engine: Engine) -> pd.DataFrame:
    return pd.read_sql_query("SELECT PROJECTID, PI_NAME, PI_INSTITUTION FROM projects", engine)


def get_projects_lookup(engine: Engine) -> dict[int, dict[str, str]]:
    df = get_projects_df(engine)
    if df.empty:
        return {}
    df["PROJECTID"] = pd.to_numeric(df["PROJECTID"], errors="coerce")
    df = df.dropna(subset=["PROJECTID"])
    if df.empty:
        return {}
    df["PROJECTID"] = df["PROJECTID"].astype(int)
    return df.set_index("PROJECTID")[["PI_NAME", "PI_INSTITUTION"]].fillna("").to_dict(orient="index")


def get_project_rundates_lookup(engine: Engine) -> dict[int, dict[str, Any]]:
    q = """
    SELECT PROJECTID, MIN(RUNDATE) AS min_date, MAX(RUNDATE) AS max_date
    FROM analysis
    WHERE PROJECTID IS NOT NULL AND RUNDATE IS NOT NULL
    GROUP BY PROJECTID
    """
    df = pd.read_sql_query(q, engine)
    if df.empty:
        return {}
    df["PROJECTID"] = pd.to_numeric(df["PROJECTID"], errors="coerce")
    df = df.dropna(subset=["PROJECTID"])
    if df.empty:
        return {}
    df["PROJECTID"] = df["PROJECTID"].astype(int)
    return df.set_index("PROJECTID")[["min_date", "max_date"]].to_dict(orient="index")


def fetch_analysis_subset(
    engine: Engine,
    *,
    testname: str,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
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
        where_parts.append("(GBA IS NULL OR CAST(GBA AS REAL) != 1.0)")

    sql = f"SELECT {select_cols} FROM analysis WHERE {' AND '.join(where_parts)}"
    out = pd.read_sql_query(text(sql), engine, params=params)
    return out

