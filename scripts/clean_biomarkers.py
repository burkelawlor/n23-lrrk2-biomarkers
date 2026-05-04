from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.biomarker_data_loading import *
from utils.data_processing import append_to_cleaned_biospecimen_csv


def _parse_args() -> argparse.Namespace:
    load_dotenv(_REPO_ROOT / ".env")
    p = argparse.ArgumentParser(
        description=(
            "Clean raw biospecimen data per project and append to an optional local MySQL DB "
            "and cleaned CSVs. Each project has its own cleaning function; see "
            "notebooks/data_cleaning.ipynb."
        )
    )
    p.add_argument(
        "--cleaner",
        type=str,
        default=None,
        help=(
            "If provided, only run this cleaner label. "
            '(examples: "--cleaner bulk-ppmi", "--cleaner ppmi-151").'
        ),
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default="data/raw",
        help="Directory containing raw CSV inputs (default: data/raw).",
    )
    p.add_argument(
        "--database-url",
        type=str,
        default=os.getenv("DATABASE_URL"),
        help=(
            "Optional SQLAlchemy DATABASE_URL to also load rows into a MySQL DB after "
            "writing cleaned CSV artifacts "
            '(example: "mysql+pymysql://user:pass@127.0.0.1:3306/biomarkers"). '
            "Defaults to env var DATABASE_URL (loaded from .env if present)."
        ),
    )
    p.add_argument(
        "--output-analysis-csv",
        type=str,
        default=None,
        help="Override output cleaned analysis CSV path.",
    )
    p.add_argument(
        "--output-projects-csv",
        type=str,
        default=None,
        help="Override output cleaned projects CSV path.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    data_dir = (_REPO_ROOT / args.data_dir).resolve()

    if args.cleaner is None:
        print("Running all cleaners...")

    ml_df = build_ml_df(data_dir)

    append_kw: dict = {
        "output_path": args.output_analysis_csv,
        "project_metadata_path": args.output_projects_csv,
        "keep": "last",
    }

    project_cleaners: list[tuple[str, Callable[..., pd.DataFrame]]] = [
        ("bulk-ppmi", clean_ppmi_bulk),
        ("ppmi-151", clean_ppmi_151),
        ("bulk-lcc", clean_lcc_bulk),
        ("lcc-122", clean_lcc_122),
    ]

    if args.cleaner is not None:
        wanted = str(args.cleaner)
        available = {label for label, _ in project_cleaners}
        if wanted not in available:
            formatted = ", ".join(sorted(available))
            raise SystemExit(
                f"--cleaner {args.cleaner!r} is not supported by this script. "
                f"Available: {formatted}"
            )
        project_cleaners = [(label, cleaner) for (label, cleaner) in project_cleaners if label == wanted]

    cleaned_by_project: dict[str, pd.DataFrame] = {}

    for label, cleaner in project_cleaners:
        print(f"Running cleaner {label}...")
        df = cleaner(data_dir, ml_df)
        if df.empty:
            print(f"Skipping project {label}: no rows after cleaning.")
            continue
        append_to_cleaned_biospecimen_csv(df, **append_kw)
        cleaned_by_project[label] = df
        print(f"Appended project {label}: {len(df)} rows.")

    if args.database_url:
        from utils.db_runtime import create_engine_from_url

        engine = create_engine_from_url(args.database_url)
        if args.cleaner is not None and str(args.cleaner) != "bulk-ppmi":
            from utils.db_ingest import replace_project_analysis_mysql, upsert_project_metadata_mysql

            label = str(args.cleaner)
            df = cleaned_by_project.get(label)
            if df is None or df.empty:
                print(f"Skipping DB upsert for project {label}: no rows after cleaning.")
            else:
                projects_csv = Path(
                    args.output_projects_csv
                    or (_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv")
                )
                projects_df = pd.read_csv(projects_csv, low_memory=False) if projects_csv.exists() else None

                for pid in df["PROJECTID"].dropna().unique():
                    pid = str(pid)
                    replace_project_analysis_mysql(engine, project_id=pid, analysis_df=df[df["PROJECTID"] == pid])
                    if projects_df is not None:
                        upsert_project_metadata_mysql(engine, projects_df=projects_df, project_id=pid)
                print(f"Upserted project {label} into MySQL.")
        else:
            from utils.db_ingest import load_csv_to_mysql

            analysis_csv = args.output_analysis_csv or str(
                _REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_analysis.csv"
            )
            projects_csv = args.output_projects_csv or str(
                _REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv"
            )
            load_csv_to_mysql(
                engine,
                analysis_csv_path=analysis_csv,
                projects_csv_path=projects_csv,
            )
            print("Loaded cleaned CSV artifacts into MySQL.")

    print("Done. Updated cleaned CSV(s).")


if __name__ == "__main__":
    main()
