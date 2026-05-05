"""Per-source cleaner functions and shared data-loading helpers for the biomarker pipeline."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def _is_float(val) -> bool:
    try:
        float(val)
        return True
    except Exception:
        return False

def _make_ratio_data(data, numerator_cols, denominator_cols, testname, subject_id_col='PATNO', event_col='CLINICAL_EVENT'):
  # Reformat dataframe
  out = data[data['TESTNAME'].isin(numerator_cols + denominator_cols)] # keep only tests of interest
  out = out.pivot(index=[subject_id_col, event_col], columns='TESTNAME', values='TESTVALUE').reset_index() # pivot
  out = out[~out.isna().any(axis=1)] # drop rows with missing values

  # Calculate ratio and format result
  out['TESTNAME'] = testname
  out['TESTVALUE'] = out[numerator_cols].sum(axis=1) / out[denominator_cols].sum(axis=1)
  out['UNITS'] = 'ratio'
  out = out[[subject_id_col, event_col,'TESTNAME','TESTVALUE','UNITS']]

  # Merge with rest of input data
  out = pd.merge(data.drop(['TESTNAME','TESTVALUE','UNITS','RUNDATE'], axis=1).drop_duplicates(), out, on=[subject_id_col,event_col], how='right')
  out['RUNDATE'] = pd.NaT

  return out


def build_ml_df(data_dir: Path) -> pd.DataFrame:
    data_dir = data_dir / "AMPPD"
    ml_df_full = pd.read_csv(data_dir / "AMPPDv4_LRRK2v4_results_N23.csv")
    ml_df_posthoc = pd.read_csv(data_dir / "AMPPDv4_LRRK2v4_results_N23_for_post_hoc.csv")
    

    ml_df_full["GBA"] = (~ml_df_full.ID.isin(ml_df_posthoc.ID)).astype(int)
    ml_df_full.rename(
        columns={
            "LRRK2-RV": "RV",
            "LRRK2-Predicted": "PREDICTED",
            "LRRK2-Driven": "DRIVEN",
            "heuristic": "HEURISTIC",
        },
        inplace=True,
    )
    ml_df_full['FOCUS_ONLY'] = np.select([ml_df_full.RV == 1, ml_df_full.flag_focus == 1], ['RV', 'Predicted'], 'Non')
    ml_df_full['READOUT_ONLY'] = np.select([ml_df_full.RV == 1, ml_df_full.flag_readout == 1], ['RV', 'Predicted'], 'Non')


    dosage_df = pd.read_csv(data_dir / "amppdv4_lrrk2v4_dosges.csv")
    ml_df_full = ml_df_full.merge(dosage_df[['ID', 'rs76904798_T']], on='ID', how='left')
    ml_df_full['rs76904798'] = (
        pd.to_numeric(ml_df_full['rs76904798_T'], errors='coerce')
        .round()
        .map({0.0: 'CC', 1.0: 'TC', 2.0: 'TT'})
    )

    amp_case_control = pd.read_csv(data_dir / "releases_2023_v4release_1027_amp_pd_case_control.csv")
    ml_df_full = ml_df_full.merge(amp_case_control, left_on='ID', right_on='participant_id', how='left').rename(columns={'case_control_other_latest':'CASE_CONTROL'})
    
    return ml_df_full[["ID", "CASE_CONTROL", "RV", "GBA", "PREDICTED", "DRIVEN", "HEURISTIC", "FOCUS_ONLY", "READOUT_ONLY", "rs76904798"]].copy()


def build_ppmi_df(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    data_dir = data_dir / "PPMI"
    ml_ppmi = ml_df[ml_df.ID.str.contains("PP-")].copy()
    ml_ppmi["PATNO"] = ml_ppmi.ID.str.strip("PP-").astype(int)

    results_df = pd.read_csv(
        data_dir / "Current_Biospecimen_Analysis_Results_06Mar2026.csv", low_memory=False
    )
    results_df = results_df.drop_duplicates(
        subset=["PATNO", "PROJECTID", "CLINICAL_EVENT", "TYPE", "TESTNAME", "RUNDATE"], keep="last"
    )

    age_df = pd.read_csv(data_dir / "Age_at_visit_24Mar2026.csv").rename(
        columns={"EVENT_ID": "CLINICAL_EVENT"}
    )
    age_df = age_df.drop_duplicates(subset=["PATNO", "CLINICAL_EVENT"], keep="last")

    ppmi_df = results_df.merge(ml_ppmi, on="PATNO", how="left")
    ppmi_df = ppmi_df.merge(age_df, on=["PATNO", "CLINICAL_EVENT"], how="left")
    ppmi_df["PROJECTID"] = "PPMI " + ppmi_df["PROJECTID"].astype(str)
    ppmi_df["PATIENTID"] = ppmi_df["ID"]
    return ppmi_df

def build_lcc_df(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    data_dir = data_dir / "LCC"
    df = pd.read_csv(data_dir / "LCC_Biomarkers_compiled_080122.csv", low_memory=False)
    ml_lcc = ml_df[ml_df.ID.str.startswith("LC-")].copy()
    ml_lcc["lrrkid"] = ml_lcc.ID.str.replace("LC-", "", regex=False)
    df = df.merge(
        ml_lcc[["lrrkid", "RV", "GBA", "PREDICTED", "DRIVEN", "HEURISTIC"]], on="lrrkid", how="left"
    )
    df["PROJECTID"] = "LCC " + df["Biomarker_projectID"].astype(str)
    df["PATNO"] = df["lrrkid"].astype(str)
    df["SEX"] = df["gender"].map({1.0: "Male", 2.0: "Female"})
    df["AGE_AT_VISIT"] = df["demopd_ageassess"]
    df["COHORT"] = df["pdenrl"].map({0.0: "Control", 1.0: "PD"})
    df.rename(columns={"EVENT": "CLINICAL_EVENT", "Biomarker_sampletype": "TYPE"}, inplace=True)
    df["TESTNAME"] = df["TESTNAME"].astype(str).str[:255]
    df["PATIENTID"] = "LC-" + df["lrrkid"].astype(str)
    return df


def clean_ppmi_bulk(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    df = build_ppmi_df(data_dir, ml_df)

    df["can_float"] = df["TESTVALUE"].apply(_is_float)
    agg_df = df.groupby("PROJECTID").agg(
        num_entries=("TESTVALUE", "size"),
        num_non_float=("can_float", lambda x: (~x).sum()),
    )
    agg_df["percent_non_float"] = agg_df["num_non_float"] / agg_df["num_entries"] * 100
    projects_to_include = agg_df[agg_df["percent_non_float"] < 10].index
    df = df[df.PROJECTID.isin(projects_to_include)].copy()

    df.loc[df["can_float"] == False, "TESTVALUE"] = np.nan  # noqa: E712
    return df

def clean_lcc_bulk(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    df = build_lcc_df(data_dir, ml_df)

    df["can_float"] = df["TESTVALUE"].apply(_is_float)
    agg_df = df.groupby("Biomarker_projectID").agg(
        num_entries=("TESTVALUE", "size"),
        num_non_float=("can_float", lambda x: (~x).sum()),
    )
    agg_df["percent_non_float"] = agg_df["num_non_float"] / agg_df["num_entries"] * 100
    projects_to_include = agg_df[agg_df["percent_non_float"] < 10].index
    df = df[df["Biomarker_projectID"].isin(projects_to_include)].copy()

    df.loc[df["can_float"] == False, "TESTVALUE"] = np.nan  # noqa: E712
    return df


def clean_ppmi_151(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    data_dir = data_dir / "PPMI"
    ml_ppmi = ml_df[ml_df.ID.str.contains("PP-")].copy()
    ml_ppmi["PATNO"] = ml_ppmi.ID.str.strip("PP-").astype(int)

    age_df = pd.read_csv(data_dir / "Age_at_visit_24Mar2026.csv").rename(
        columns={"EVENT_ID": "CLINICAL_EVENT"}
    )
    age_df = age_df.drop_duplicates(subset=["PATNO", "CLINICAL_EVENT"], keep="last")

    files_151 = sorted(data_dir.glob("Project_151_pQTL_in_CSF_*_of_7_Batch_Corrected__*.csv"))
    if not files_151:
        return pd.DataFrame()

    project_151 = pd.concat(
        [pd.read_csv(f, low_memory=False) for f in files_151], ignore_index=True
    )

    key = pd.read_csv(
        data_dir / "PPMI_Project_151_pqtl_Analysis_Annotations_20210210.csv",
        usecols=["SOMA_SEQ_ID", "TARGET_GENE_SYMBOL"],
    )
    key["TESTNAME_2"] = (
        key["TARGET_GENE_SYMBOL"].astype("string") + "_" + key["SOMA_SEQ_ID"].astype("string")
    )
    key["TESTNAME_2"] = key["TESTNAME_2"].fillna(key["SOMA_SEQ_ID"])
    key = key.drop_duplicates()

    project_151 = project_151.merge(key, left_on="TESTNAME", right_on=["SOMA_SEQ_ID"], how="left")
    project_151["TESTNAME"] = project_151["TESTNAME_2"]
    project_151["PROJECTID"] = "PPMI 151"

    project_151 = project_151.merge(ml_ppmi, on="PATNO", how="left")
    project_151 = project_151.merge(age_df, on=["PATNO", "CLINICAL_EVENT"], how="left")
    project_151["PATIENTID"] = project_151["ID"]
    return project_151


def clean_lcc_122(data_dir: Path, ml_df: pd.DataFrame) -> pd.DataFrame:
    df = clean_lcc_bulk(data_dir, ml_df)
    df = df[df.PROJECTID == 'LCC 122'].copy()
    df = df[df.UNITS == 'area ratio'].copy()
    df['TESTVALUE'] = df['TESTVALUE'].astype(float)

    r1 = _make_ratio_data(df, ['GlcCer (d18:1, 16:0)','GlcCer (d18:1, 18:0)','GlcCer (d18:1, 24:0)','GlcCer (d18:1, 24:1)'], ['Cer(d18:1/16:0)','Cer(d18:1/18:0)','Cer(d18:1/24:0)','Cer(d18:1/24:1)'], 'GlcCer/Cer')
    r2 = _make_ratio_data(df, ['Cer(d18:1/16:0)','Cer(d18:1/18:0)','Cer(d18:1/24:0)','Cer(d18:1/24:1)'], ['SM(d18:1/16:0)', 'SM(d18:1/18:0)', 'SM(d18:1/24:0)' ,'SM(d18:1/24:1)'], 'Cer/SM')
    r3 = _make_ratio_data(df, ['LPC(16:0)', 'LPC(16:1)', 'LPC(18:0)', 'LPC(18:1)', 'LPC(20:4)', 'LPC(22:6)', 'LPC(24:0)', 'LPC(24:1)', 'LPC(26:1)'], ['PC(36:1)', 'PC(36:2)', 'PC(36:4)', 'PC(38:4)', 'PC(38:6)', 'PC(40:6)'], 'LPC/PC')
    r4 = _make_ratio_data(df, ['LPE(16:0)', 'LPE(18:0)'], ['PE(36:1)', 'PE(36:4)','PE(38:4)', 'PE(38:6)', 'PE(40:6)'], 'LPE/PE')
    return pd.concat([df, r1, r2, r3, r4])
