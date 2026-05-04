from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import Engine, text


def init_schema(engine: Engine) -> None:
    """
    Create the MySQL schema used by the dashboard.

    Tables:
    - projects(PROJECTID, PI_NAME, PI_INSTITUTION)
    - analysis(long-form biomarker rows + flags)
    """
    stmts = [
        # projects
        """
        CREATE TABLE IF NOT EXISTS projects (
            PROJECTID VARCHAR(64) NOT NULL,
            PI_NAME TEXT NULL,
            PI_INSTITUTION TEXT NULL,
            PRIMARY KEY (PROJECTID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        # analysis
        """
        CREATE TABLE IF NOT EXISTS analysis (
            PATNO INT NULL,
            SEX VARCHAR(32) NULL,
            AGE_AT_VISIT DOUBLE NULL,
            COHORT VARCHAR(64) NULL,
            CLINICAL_EVENT VARCHAR(64) NULL,
            TYPE VARCHAR(64) NULL,
            TESTNAME VARCHAR(255) NULL,
            TESTVALUE DOUBLE NULL,
            UNITS VARCHAR(64) NULL,
            RUNDATE DATE NULL,
            PROJECTID VARCHAR(64) NULL,
            RV DOUBLE NULL,
            GBA DOUBLE NULL,
            PREDICTED DOUBLE NULL,
            DRIVEN DOUBLE NULL,
            HEURISTIC VARCHAR(64) NULL,
            FOCUS_ONLY VARCHAR(64) NULL,
            READOUT_ONLY VARCHAR(64) NULL,
            UNIQUE KEY uq_analysis_dedupe (PATNO, PROJECTID, TESTNAME, CLINICAL_EVENT, TYPE, RUNDATE),
            KEY idx_analysis_testname (TESTNAME),
            KEY idx_analysis_projectid (PROJECTID),
            KEY idx_analysis_cohort (COHORT),
            KEY idx_analysis_heuristic (HEURISTIC),
            KEY idx_analysis_rundate (RUNDATE)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    ]

    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


def _to_records(df: pd.DataFrame, *, columns: Iterable[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    sub = df.loc[:, [c for c in columns if c in df.columns]].copy()
    # Important: keep Python None values as None (not re-coerced to NaN) so MySQL
    # drivers don't error with "nan can not be used with MySQL".
    sub = sub.astype(object).where(pd.notna(sub), None)
    return sub.to_dict("records")


def upsert_projects_mysql(engine: Engine, projects_df: pd.DataFrame) -> None:
    if projects_df.empty:
        return
    if "PROJECTID" not in projects_df.columns:
        raise ValueError("projects upsert requires a PROJECTID column.")

    dfc = projects_df.copy()
    dfc = dfc.dropna(subset=["PROJECTID"])
    if dfc.empty:
        return
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(str)

    rows = _to_records(dfc, columns=["PROJECTID", "PI_NAME", "PI_INSTITUTION"])
    if not rows:
        return

    sql = text(
        """
        INSERT INTO projects (PROJECTID, PI_NAME, PI_INSTITUTION)
        VALUES (:PROJECTID, :PI_NAME, :PI_INSTITUTION)
        ON DUPLICATE KEY UPDATE
            PI_NAME=VALUES(PI_NAME),
            PI_INSTITUTION=VALUES(PI_INSTITUTION)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def insert_analysis_ignore_duplicates_mysql(engine: Engine, analysis_df: pd.DataFrame) -> None:
    if analysis_df.empty:
        return

    required = {"PATNO", "PROJECTID", "TESTNAME", "CLINICAL_EVENT", "TYPE"}
    missing = required - set(analysis_df.columns)
    if missing:
        raise ValueError(f"analysis insert is missing required columns: {sorted(missing)}")

    dfc = analysis_df.copy()
    dfc["PATNO"] = pd.to_numeric(dfc["PATNO"], errors="coerce")
    dfc = dfc.dropna(subset=["PATNO", "PROJECTID", "TESTNAME", "CLINICAL_EVENT", "TYPE"])
    if dfc.empty:
        return
    dfc["PATNO"] = dfc["PATNO"].astype(int)
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(str)

    # MySQL schema stores RUNDATE as DATE; computed rows (e.g. ratios) have no RUNDATE.
    dfc["RUNDATE"] = pd.to_datetime(dfc["RUNDATE"], errors="coerce").dt.date
    # Python-level dedup for null-RUNDATE rows (MySQL unique key allows multiple NULLs).
    dfc = dfc.drop_duplicates(
        subset=["PATNO", "PROJECTID", "TESTNAME", "UNITS", "CLINICAL_EVENT", "TYPE"],
        keep="last",
    )
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
        "FOCUS_ONLY",
        "READOUT_ONLY",
    ]
    rows = _to_records(dfc, columns=cols)
    if not rows:
        return

    insert_sql = text(
        """
        INSERT IGNORE INTO analysis (
            PATNO, SEX, AGE_AT_VISIT, COHORT, CLINICAL_EVENT, TYPE, TESTNAME, TESTVALUE,
            UNITS, RUNDATE, PROJECTID, RV, GBA, PREDICTED, DRIVEN, HEURISTIC, FOCUS_ONLY, READOUT_ONLY
        )
        VALUES (
            :PATNO, :SEX, :AGE_AT_VISIT, :COHORT, :CLINICAL_EVENT, :TYPE, :TESTNAME, :TESTVALUE,
            :UNITS, :RUNDATE, :PROJECTID, :RV, :GBA, :PREDICTED, :DRIVEN, :HEURISTIC, :FOCUS_ONLY, :READOUT_ONLY
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)


def upsert_project_metadata_mysql(engine: Engine, *, projects_df: pd.DataFrame, project_id: str) -> None:
    if projects_df.empty:
        return
    if "PROJECTID" not in projects_df.columns:
        return
    dfc = projects_df.copy()
    dfc = dfc.dropna(subset=["PROJECTID"])
    if dfc.empty:
        return
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(str)
    dfc = dfc.loc[dfc["PROJECTID"] == project_id].copy()
    if dfc.empty:
        return
    upsert_projects_mysql(engine, dfc)


def replace_project_analysis_mysql(engine: Engine, *, project_id: str, analysis_df: pd.DataFrame) -> None:
    """
    Replace all `analysis` rows for a single PROJECTID.

    Intended for fast incremental updates when only one project was re-cleaned.
    """
    init_schema(engine)
    if analysis_df.empty:
        return
    if "PROJECTID" not in analysis_df.columns:
        raise ValueError("analysis replace requires a PROJECTID column.")

    dfc = analysis_df.copy()
    dfc = dfc.dropna(subset=["PROJECTID"])
    if dfc.empty:
        return
    dfc["PROJECTID"] = dfc["PROJECTID"].astype(str)
    dfc = dfc.loc[dfc["PROJECTID"] == project_id].copy()
    if dfc.empty:
        return

    # Align with insert validation rules and MySQL DATE storage.
    required = {"PATNO", "PROJECTID", "TESTNAME", "CLINICAL_EVENT", "TYPE"}
    missing = required - set(dfc.columns)
    if missing:
        raise ValueError(f"analysis replace is missing required columns: {sorted(missing)}")

    dfc["PATNO"] = pd.to_numeric(dfc["PATNO"], errors="coerce")
    dfc = dfc.dropna(subset=["PATNO"])
    if dfc.empty:
        # Still delete prior rows so the DB mirrors the empty result for this project.
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM analysis WHERE PROJECTID = :project_id"), {"project_id": project_id})
        return
    dfc["PATNO"] = dfc["PATNO"].astype(int)
    # Computed rows (e.g. ratios) have no RUNDATE; keep them as NULL in MySQL.
    dfc["RUNDATE"] = pd.to_datetime(dfc["RUNDATE"], errors="coerce").dt.date

    # De-duplicate within the project; use UNITS instead of RUNDATE so computed
    # ratio rows (RUNDATE=NULL) are properly collapsed.
    dfc = dfc.drop_duplicates(
        subset=["PATNO", "PROJECTID", "TESTNAME", "UNITS", "CLINICAL_EVENT", "TYPE"],
        keep="last",
    )

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
        "FOCUS_ONLY",
        "READOUT_ONLY",
    ]
    rows = _to_records(dfc, columns=cols)
    delete_sql = text("DELETE FROM analysis WHERE PROJECTID = :project_id")
    insert_sql = text(
        """
        INSERT INTO analysis (
            PATNO, SEX, AGE_AT_VISIT, COHORT, CLINICAL_EVENT, TYPE, TESTNAME, TESTVALUE,
            UNITS, RUNDATE, PROJECTID, RV, GBA, PREDICTED, DRIVEN, HEURISTIC, FOCUS_ONLY, READOUT_ONLY
        )
        VALUES (
            :PATNO, :SEX, :AGE_AT_VISIT, :COHORT, :CLINICAL_EVENT, :TYPE, :TESTNAME, :TESTVALUE,
            :UNITS, :RUNDATE, :PROJECTID, :RV, :GBA, :PREDICTED, :DRIVEN, :HEURISTIC, :FOCUS_ONLY, :READOUT_ONLY
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(delete_sql, {"project_id": project_id})
        if rows:
            conn.execute(insert_sql, rows)


def load_csv_to_mysql(
    engine: Engine,
    *,
    analysis_csv_path: Path | str,
    projects_csv_path: Path | str | None = None,
    chunksize: int = 50_000,
) -> None:
    """
    Bulk-load CSV artifacts into MySQL.

    - Ensures schema exists
    - Loads projects (upsert) if projects CSV provided and exists
    - Loads analysis rows using INSERT IGNORE to respect the unique dedupe key
    """
    print("Initializing schema...")
    init_schema(engine)

    projects_path = Path(projects_csv_path) if projects_csv_path is not None else None
    if projects_path is not None and projects_path.exists():
        print(f"Loading and upserting projects from: {projects_path}")
        projects = pd.read_csv(projects_path, low_memory=False)
        upsert_projects_mysql(engine, projects)
        print(f"Projects upserted.")

    analysis_path = Path(analysis_csv_path)
    if not analysis_path.exists():
        raise FileNotFoundError(f"Missing analysis CSV: {analysis_path}")

    print(f"Loading analysis CSV in chunks from: {analysis_path}")
    # Count number of lines for progress, skipping header
    with open(analysis_path, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f) - 1
    num_chunks = (total_lines // int(chunksize)) + (1 if total_lines % int(chunksize) else 0)

    for idx, chunk in enumerate(
        pd.read_csv(analysis_path, low_memory=False, chunksize=int(chunksize)), 1
    ):
        insert_analysis_ignore_duplicates_mysql(engine, chunk)
        print(f"Inserted chunk {idx}/{num_chunks}")

    print("All analysis chunks processed.")

