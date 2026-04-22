from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
import numpy as np

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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


def _build_ml_df(data_dir: Path) -> pd.DataFrame:
    ml_full_path = data_dir / "AMPPDv4_LRRK2v4_results_N23.csv"
    ml_posthoc_path = data_dir / "AMPPDv4_LRRK2v4_results_N23_for_post_hoc.csv"
    ml_df_full = pd.read_csv(ml_full_path)
    ml_df_posthoc = pd.read_csv(ml_posthoc_path)

    ml_df_full['GBA'] = (~ml_df_full.ID.isin(ml_df_posthoc.ID)).astype(int)
    ml_df_full.rename(columns={'LRRK2-RV': 'RV', 'LRRK2-Predicted': 'PREDICTED', 'LRRK2-Driven': 'DRIVEN', 'heuristic': 'HEURISTIC'}, inplace=True)

    return ml_df_full[['ID', 'RV', 'GBA', 'PREDICTED', 'DRIVEN', 'HEURISTIC']].copy()


def _build_ppmi_df(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    
    ml_ppmi = ml_df[ml_df.ID.str.contains('PP-')]
    ml_ppmi['PATNO'] = ml_ppmi.ID.str.strip('PP-').astype(int)
    
    results_path = data_dir / "Current_Biospecimen_Analysis_Results_06Mar2026.csv"
    results_df = pd.read_csv(results_path, low_memory=False)
    results_df = results_df.drop_duplicates(subset=["PATNO", "PROJECTID", "CLINICAL_EVENT", "TYPE", "TESTNAME", "RUNDATE"], keep="last")
    
    age_path = data_dir / "Age_at_visit_24Mar2026.csv"
    age_df = pd.read_csv(age_path).rename(columns={"EVENT_ID": "CLINICAL_EVENT"})
    age_df = age_df.drop_duplicates(subset=["PATNO", "CLINICAL_EVENT"], keep="last")
    
    ppmi_df = results_df.merge(ml_ppmi, on="PATNO", how="left")
    ppmi_df = ppmi_df.merge(age_df, on=["PATNO", "CLINICAL_EVENT"], how="left")

    ppmi_df['PROJECTID'] = 'PPMI ' + ppmi_df['PROJECTID'].astype(str)

    return ppmi_df


def _is_float(val):
    try:
        float(val)
        return True
    except Exception:
        return False

def clean_ppmi_bulk(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    
    df = _build_ppmi_df(data_dir, ml_df)

    # Subset to projects where >10% of values cannot be converted to floats
    df["can_float"] = df["TESTVALUE"].apply(_is_float)
    agg_df = df.groupby("PROJECTID").agg(
        num_entries=("TESTVALUE", "size"),
        num_non_float=("can_float", lambda x: (~x).sum()),
    )
    agg_df["percent_non_float"] = agg_df["num_non_float"] / agg_df["num_entries"] * 100
    projects_to_include = agg_df[agg_df["percent_non_float"] < 10].index
    df = df[df.PROJECTID.isin(projects_to_include)].copy()
    
    # Replace all nonfloat with nan
    df.loc[df["can_float"] == False, "TESTVALUE"] = np.nan
    return df

def clean_ppmi_151(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    ml_ppmi = ml_df[ml_df.ID.str.contains('PP-')]
    ml_ppmi['PATNO'] = ml_ppmi.ID.str.strip('PP-').astype(int)
    
    age_path = data_dir / "Age_at_visit_24Mar2026.csv"
    age_df = pd.read_csv(age_path).rename(columns={"EVENT_ID": "CLINICAL_EVENT"})
    age_df = age_df.drop_duplicates(subset=["PATNO", "CLINICAL_EVENT"], keep="last")
    
    # Load the biospec results
    files_151 = sorted(data_dir.glob("Project_151_pQTL_in_CSF_*_of_7_Batch_Corrected__*.csv"))
    if not files_151:
        return pd.DataFrame()

    project_151 = pd.concat([pd.read_csv(f, low_memory=False) for f in files_151], ignore_index=True)

    # Join annotations to construct the notebook's TESTNAME_2 and replace TESTNAME.
    key_path = data_dir / "PPMI_Project_151_pqtl_Analysis_Annotations_20210210.csv"
    key = pd.read_csv(key_path, usecols=["SOMA_SEQ_ID", "TARGET_GENE_SYMBOL"])
    key["TESTNAME_2"] = key["TARGET_GENE_SYMBOL"].astype("string") + "_" + key["SOMA_SEQ_ID"].astype(
        "string"
    )
    key["TESTNAME_2"] = key["TESTNAME_2"].fillna(key["SOMA_SEQ_ID"])
    key = key.drop_duplicates()

    project_151 = project_151.merge(key, left_on="TESTNAME", right_on=["SOMA_SEQ_ID"], how="left")
    project_151["TESTNAME"] = project_151["TESTNAME_2"]
    project_151['PROJECTID'] = 'PPMI 151'

    project_151 = project_151.merge(ml_ppmi, on="PATNO", how="left")
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

def clean_lcc_bulk(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    lcc_path = data_dir / "LCC_Biomarkers_compiled_080122.csv"
    df = pd.read_csv(lcc_path, low_memory=False)

    # Build LCC-specific ml_df slice (LC- prefix IDs)
    ml_lcc = ml_df[ml_df.ID.str.startswith("LC-")].copy()
    ml_lcc["lrrkid"] = ml_lcc.ID.str.replace("LC-", "", regex=False)

    # Merge ml scores onto LCC data via lrrkid
    df = df.merge(ml_lcc[["lrrkid", "RV", "GBA", "PREDICTED", "DRIVEN", "HEURISTIC"]], on="lrrkid", how="left")

    # Subset to projects where <10% of values cannot be converted to floats
    df["can_float"] = df["TESTVALUE"].apply(_is_float)
    agg_df = df.groupby("Biomarker_projectID").agg(
        num_entries=("TESTVALUE", "size"),
        num_non_float=("can_float", lambda x: (~x).sum()),
    )
    agg_df["percent_non_float"] = agg_df["num_non_float"] / agg_df["num_entries"] * 100
    projects_to_include = agg_df[agg_df["percent_non_float"] < 10].index
    df = df[df["Biomarker_projectID"].isin(projects_to_include)].copy()

    # Replace non-float values with NaN
    df.loc[df["can_float"] == False, "TESTVALUE"] = np.nan

    # Map to canonical columns
    df["PROJECTID"] = "LCC " + df["Biomarker_projectID"].astype(str)
    df["PATNO"] = df["lrrkid"].astype(str)
    df["SEX"] = df["gender"].map({1.0: "Male", 2.0: "Female"})
    df["AGE_AT_VISIT"] = df["demopd_ageassess"]
    df["COHORT"] = df["pdenrl"].map({0.0: "Control", 1.0: "PD"})
    df.rename(columns={"EVENT": "CLINICAL_EVENT", "Biomarker_sampletype": "TYPE"}, inplace=True)

    # Truncate TESTNAME to match the DB VARCHAR(255) column limit
    df["TESTNAME"] = df["TESTNAME"].astype(str).str[:255]

    return df


def main() -> None:
    args = _parse_args()
    data_dir = (_REPO_ROOT / args.data_dir).resolve()

    if args.cleaner is None:
        print("Running all cleaners...")

    ml_df = _build_ml_df(data_dir)

    append_kw: dict = {
        "output_path": args.output_analysis_csv,
        "project_metadata_path": args.output_projects_csv,
        "keep": "last",
    }

    project_cleaners: list[tuple[str, Callable[..., pd.DataFrame]]] = [
        ("bulk-ppmi", clean_ppmi_bulk),
        ("ppmi-151", clean_ppmi_151),
        ("bulk-lcc", clean_lcc_bulk),
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
