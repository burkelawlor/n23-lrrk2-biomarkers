from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.data_processing import append_to_cleaned_biospecimen_csv


def _pick_latest(data_dir: Path, pattern: str) -> Path:
    matches = sorted(data_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files match {pattern!r} in {data_dir}")
    return matches[-1]


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
        "--projectid",
        type=int,
        default=None,
        help=(
            "If provided, only run the cleaner for this PPMI PROJECTID "
            "(e.g. --projectid 145 runs only clean_project_145)."
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


def _build_ml_df(data_dir: Path) -> pd.DataFrame:
    ml_full_path = data_dir / "AMPPDv4_LRRK2v4_results_N23.csv"
    ml_posthoc_path = data_dir / "AMPPDv4_LRRK2v4_results_N23_for_post_hoc.csv"
    ml_df_full = pd.read_csv(ml_full_path)
    ml_df_posthoc = pd.read_csv(ml_posthoc_path)

    ml_df_full = ml_df_full.copy()
    ml_df_full["GBA"] = (~ml_df_full.ID.isin(ml_df_posthoc.ID)).astype(int)

    ml_df = ml_df_full.loc[ml_df_full.ID.astype(str).str.contains("PP-")].copy()
    ml_df["PATNO"] = ml_df["ID"].astype(str).str.strip("PP-").astype(int)
    ml_df = ml_df.rename(
        columns={
            "LRRK2-RV": "RV",
            "LRRK2-Predicted": "PREDICTED",
            "LRRK2-Driven": "DRIVEN",
            "heuristic": "HEURISTIC",
        }
    )
    return ml_df.loc[:, ["PATNO", "RV", "GBA", "PREDICTED", "DRIVEN", "HEURISTIC"]].copy()


def _build_age_df(data_dir: Path) -> pd.DataFrame:
    age_path = _pick_latest(data_dir, "Age_at_visit_*.csv")
    age = pd.read_csv(age_path).rename(columns={"EVENT_ID": "CLINICAL_EVENT"})
    age = age.drop_duplicates(subset=["PATNO", "CLINICAL_EVENT"], keep="last")
    return age


def _load_biospecimen_results(data_dir: Path) -> pd.DataFrame:
    results_path = _pick_latest(data_dir, "Current_Biospecimen_Analysis_Results_*.csv")
    return pd.read_csv(results_path, low_memory=False)


def _dedupe_biomarker_rows(df: pd.DataFrame) -> pd.DataFrame:
    dff = df.copy()
    dff = dff.drop_duplicates(
        subset=["PATNO", "PROJECTID", "CLINICAL_EVENT", "TYPE", "TESTNAME", "RUNDATE"],
        keep="last",
    )
    return dff


def clean_project_105(biomarker_df: pd.DataFrame) -> pd.DataFrame:
    project_105 = biomarker_df.loc[biomarker_df["PROJECTID"] == 105].copy()
    return project_105

def clean_project_145(biomarker_df: pd.DataFrame) -> pd.DataFrame:
    project_145 = biomarker_df.loc[biomarker_df["PROJECTID"] == 145].copy()
    return project_145


def clean_project_151(data_dir: Path, ml_df: pd.DataFrame, age_df: pd.DataFrame) -> pd.DataFrame:
    files_151 = sorted(data_dir.glob("Project_151_pQTL_in_CSF_*_of_7_Batch_Corrected__*.csv"))
    if not files_151:
        return pd.DataFrame()

    project_151 = pd.concat([pd.read_csv(f, low_memory=False) for f in files_151], ignore_index=True)

    # Join annotations to construct the notebook's TESTNAME_2 and replace TESTNAME.
    key_path = data_dir / "PPMI_Project_151_pqtl_Analysis_Annotations_20210210.csv"
    key = pd.read_csv(key_path, usecols=["SOMA_SEQ_ID", "TARGET_GENE_SYMBOL"])
    key = key.copy()
    key["TESTNAME_2"] = key["TARGET_GENE_SYMBOL"].astype("string") + "_" + key["SOMA_SEQ_ID"].astype(
        "string"
    )
    key["TESTNAME_2"] = key["TESTNAME_2"].fillna(key["SOMA_SEQ_ID"])
    key = key.drop_duplicates()

    project_151 = project_151.merge(key, left_on="TESTNAME", right_on=["SOMA_SEQ_ID"], how="left")
    project_151["TESTNAME"] = project_151["TESTNAME_2"]

    project_151 = project_151.merge(ml_df, on="PATNO", how="left")
    project_151 = project_151.merge(age_df, on=["PATNO", "CLINICAL_EVENT"], how="left")

    # curated = {
    #     "C1QTNF1_6304-8_3",
    #     "HLA-DQA2_7757-5_3",
    #     "GPNMB_8240-207_3",
    #     "GRN_4992-49_1",
    #     "GPNMB_5080-131_3",
    #     "ITGB2_12750-9_3",
    #     "ENTPD1_3182-38_2",
    # }
    # project_151 = project_151.loc[project_151["TESTNAME"].astype(str).isin(curated)].copy()
    return project_151


def main() -> None:
    args = _parse_args()
    data_dir = (_REPO_ROOT / args.data_dir).resolve()

    if args.projectid is not None:
        print(f"Running cleaner for project {args.projectid}.")
    else:
        print("Running cleaner for all projects.")

    ml_df = _build_ml_df(data_dir)
    age_df = _build_age_df(data_dir)

    biomarker_df = _load_biospecimen_results(data_dir)
    biomarker_df = _dedupe_biomarker_rows(biomarker_df)
    biomarker_df = biomarker_df.merge(ml_df, on="PATNO", how="left")
    biomarker_df = biomarker_df.merge(age_df, on=["PATNO", "CLINICAL_EVENT"], how="left")

    append_kw: dict = {
        "output_path": args.output_analysis_csv,
        "project_metadata_path": args.output_projects_csv,
        "keep": "last",
    }

    project_cleaners: list[tuple[str, Callable[..., pd.DataFrame]]] = [
        ("145", clean_project_145),
        ("151", clean_project_151),
        ("105", clean_project_105),
    ]

    if args.projectid is not None:
        wanted = str(args.projectid)
        available = {label for label, _ in project_cleaners}
        if wanted not in available:
            raise SystemExit(
                f"--projectid {args.projectid} is not supported by this script. "
                f"Available: {', '.join(sorted(available, key=int))}"
            )
        project_cleaners = [(label, cleaner) for (label, cleaner) in project_cleaners if label == wanted]

    cleaned_by_project: dict[str, pd.DataFrame] = {}

    for label, cleaner in project_cleaners:
        if label == "151":
            df = cleaner(data_dir, ml_df, age_df)
        else:
            df = cleaner(biomarker_df)
        if df.empty:
            print(f"Skipping project {label}: no rows after cleaning.")
            continue
        append_to_cleaned_biospecimen_csv(df, **append_kw)
        cleaned_by_project[label] = df
        print(f"Appended project {label}: {len(df)} rows.")

    if args.database_url:
        from utils.db_runtime import create_engine_from_url

        engine = create_engine_from_url(args.database_url)
        if args.projectid is not None:
            from utils.db_ingest import replace_project_analysis_mysql, upsert_project_metadata_mysql

            label = str(args.projectid)
            df = cleaned_by_project.get(label)
            if df is None or df.empty:
                print(f"Skipping DB upsert for project {label}: no rows after cleaning.")
            else:
                replace_project_analysis_mysql(engine, project_id=int(args.projectid), analysis_df=df)

                projects_csv = Path(
                    args.output_projects_csv
                    or (_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv")
                )
                if projects_csv.exists():
                    projects_df = pd.read_csv(projects_csv, low_memory=False)
                    upsert_project_metadata_mysql(
                        engine, projects_df=projects_df, project_id=int(args.projectid)
                    )
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
