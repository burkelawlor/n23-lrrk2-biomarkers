from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import yaml

import numpy as np
import pandas as pd
from utils.biomarker_regression import run_biomarker_by_biomarker_cohort_regressions


@dataclass(frozen=True)
class RegressionConfig:
    gba_included: bool
    log_transform: bool
    pd_only: bool


def _bh_fdr(pvals: pd.Series) -> pd.Series:
    """
    Benjamini-Hochberg FDR correction.

    NaNs are preserved as NaNs.
    """
    p = pvals.astype(float).copy()
    out = pd.Series(np.nan, index=p.index, dtype=float)

    mask = p.notna()
    if int(mask.sum()) == 0:
        return out

    p_nonan = p.loc[mask].sort_values()
    m = len(p_nonan)
    ranks = np.arange(1, m + 1, dtype=float)
    adj = p_nonan.values * m / ranks
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    out.loc[p_nonan.index] = np.clip(adj, 0.0, 1.0)
    return out


def _parse_bool(value: Any, *, field_name: str, context: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{context}: expected {field_name} to be a bool, got {type(value).__name__}")


def _parse_config(obj: Any, *, context: str) -> RegressionConfig:
    if obj is None:
        raise ValueError(f"{context}: expected a mapping with keys gba_included/log_transform, got null")
    if not isinstance(obj, dict):
        raise ValueError(f"{context}: expected a mapping, got {type(obj).__name__}")

    allowed = {"gba_included", "log_transform", "pd_only"}
    extra = sorted([k for k in obj.keys() if k not in allowed])
    if extra:
        raise ValueError(f"{context}: unknown keys {extra}; allowed keys are {sorted(allowed)}")

    gba_included = _parse_bool(obj.get("gba_included", False), field_name="gba_included", context=context)
    log_transform = _parse_bool(obj.get("log_transform", False), field_name="log_transform", context=context)
    pd_only = _parse_bool(obj.get("pd_only", True), field_name="pd_only", context=context)
    return RegressionConfig(gba_included=gba_included, log_transform=log_transform, pd_only=pd_only)


def _load_yaml(path: Path) -> dict[str, Any]:

    if not path.exists():
        raise FileNotFoundError(f"Config YAML not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML must be a mapping, got {type(data).__name__}")
    return data


def _coerce_projectids_map(obj: Any) -> dict[int, dict[str, Any]]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise ValueError(f"projectids must be a mapping, got {type(obj).__name__}")
    out: dict[int, dict[str, Any]] = {}
    for k, v in obj.items():
        try:
            pid = int(k)
        except Exception as exc:
            raise ValueError(f"projectids key {k!r} is not coercible to int") from exc
        if v is None:
            out[pid] = {}
            continue
        if not isinstance(v, dict):
            raise ValueError(f"projectids[{k!r}] must be a mapping, got {type(v).__name__}")
        out[pid] = v
    return out


def _coerce_testnames_map(obj: Any) -> dict[str, dict[str, Any]]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise ValueError(f"testnames must be a mapping, got {type(obj).__name__}")
    out: dict[str, dict[str, Any]] = {}
    for k, v in obj.items():
        name = str(k)
        if v is None:
            out[name] = {}
            continue
        if not isinstance(v, dict):
            raise ValueError(f"testnames[{name!r}] must be a mapping, got {type(v).__name__}")
        out[name] = v
    return out


def _load_regression_configs(path: Path) -> tuple[RegressionConfig, dict[int, RegressionConfig], dict[str, RegressionConfig]]:
    data = _load_yaml(path)

    allowed_top = {"global", "projectids", "testnames"}
    extra_top = sorted([k for k in data.keys() if k not in allowed_top])
    if extra_top:
        raise ValueError(f"Unknown top-level keys in YAML: {extra_top}. Allowed: {sorted(allowed_top)}")

    global_cfg = _parse_config(data.get("global", {}), context="global")
    projectids_raw = _coerce_projectids_map(data.get("projectids"))
    testnames_raw = _coerce_testnames_map(data.get("testnames"))

    project_cfgs: dict[int, RegressionConfig] = {}
    for pid, cfg_obj in projectids_raw.items():
        merged = {
            "gba_included": global_cfg.gba_included,
            "log_transform": global_cfg.log_transform,
            "pd_only": global_cfg.pd_only,
        }
        merged.update(cfg_obj)
        project_cfgs[pid] = _parse_config(merged, context=f"projectids[{pid}]")

    test_cfgs: dict[str, RegressionConfig] = {}
    for testname, cfg_obj in testnames_raw.items():
        merged = {
            "gba_included": global_cfg.gba_included,
            "log_transform": global_cfg.log_transform,
            "pd_only": global_cfg.pd_only,
        }
        merged.update(cfg_obj)
        test_cfgs[testname] = _parse_config(merged, context=f"testnames[{testname!r}]")

    return global_cfg, project_cfgs, test_cfgs


def _effective_config(
    *,
    project_id: int,
    testname: str,
    global_cfg: RegressionConfig,
    project_cfgs: dict[int, RegressionConfig],
    test_cfgs: dict[str, RegressionConfig],
) -> RegressionConfig:
    if testname in test_cfgs:
        return test_cfgs[testname]
    if project_id in project_cfgs:
        return project_cfgs[project_id]
    return global_cfg


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Run biomarker-by-cohort regressions per PROJECTID so BH-FDR is computed "
            "within each project. Outputs are appended to CSVs with de-duplication."
        )
    )
    p.add_argument(
        "--projectid",
        type=int,
        default=None,
        help="If provided, run only this PROJECTID (e.g. --projectid 145).",
    )
    p.add_argument(
        "--config-yaml",
        type=str,
        default=None,
        help=(
            "Path to regression_configs.yaml controlling preprocessing. "
            "If not provided, defaults to ./regression_configs.yaml (repo root)."
        ),
    )
    return p.parse_args()


def _read_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def _apply_gba_filter(df: pd.DataFrame, *, gba_included: bool) -> pd.DataFrame:
    if gba_included:
        return df
    if "GBA" not in df.columns:
        raise ValueError("Expected a GBA column for gba_included=false filtering.")
    dff = df.copy()
    dff["GBA"] = pd.to_numeric(dff["GBA"], errors="coerce")
    return dff.loc[dff["GBA"] != 1].copy()


def _apply_log_transform(df: pd.DataFrame, *, log_transform: bool) -> pd.DataFrame:
    if not log_transform:
        return df
    if "TESTVALUE" not in df.columns:
        raise ValueError("Expected a TESTVALUE column for log_transform=true.")
    dff = df.copy()
    dff["TESTVALUE"] = pd.to_numeric(dff["TESTVALUE"], errors="coerce")
    dff = dff.loc[dff["TESTVALUE"] > 0].copy()
    dff["LOG_TESTVALUE"] = np.log(dff["TESTVALUE"].astype(float))
    return dff


def _apply_pd_only_filter(df: pd.DataFrame, *, pd_only: bool) -> pd.DataFrame:
    if not pd_only:
        return df
    if "COHORT" not in df.columns:
        raise ValueError("Expected a COHORT column for pd_only=true filtering.")
    return df.loc[df["COHORT"].astype(str) == "PD"].copy()


def main() -> None:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent
    input_csv = repo_root / "data" / "processed" / "cleaned_biospecimen_analysis.csv"
    output_dir = repo_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    config_path = Path(args.config_yaml) if args.config_yaml else (repo_root / "regression_configs.yaml")
    global_cfg, project_cfgs, test_cfgs = _load_regression_configs(config_path)
    print(f"Loaded regression config YAML: {config_path}")

    df = pd.read_csv(input_csv)
    print(f"Loaded {input_csv} with shape={df.shape}")

    if "PROJECTID" not in df.columns:
        raise ValueError(
            f"Expected a PROJECTID column in {input_csv} for per-project regressions."
        )

    df = df.copy()
    df["PROJECTID"] = pd.to_numeric(df["PROJECTID"], errors="coerce")
    df = df.dropna(subset=["PROJECTID"])
    df["PROJECTID"] = df["PROJECTID"].astype(int)

    if args.projectid is not None:
        df = df.loc[df["PROJECTID"] == int(args.projectid)].copy()
        if df.empty:
            raise ValueError(f"No rows found for PROJECTID={args.projectid}.")

    omnibus_parts: list[pd.DataFrame] = []
    pairwise_parts: list[pd.DataFrame] = []

    grouped = df.groupby("PROJECTID", sort=True)
    for project_id, sub in grouped:
        cohort_col = "HEURISTIC"
        if cohort_col not in sub.columns:
            raise ValueError(f"Expected {cohort_col!r} column for cohort comparisons.")
        if sub[cohort_col].nunique(dropna=True) < 2:
            print(
                f"Skipping PROJECTID={project_id}: need >=2 cohort values, "
                f"got {sub[cohort_col].nunique(dropna=True)}"
            )
            continue

        if "TESTNAME" not in sub.columns:
            raise ValueError("Expected a TESTNAME column for per-biomarker regressions.")

        biomarkers = sorted(sub["TESTNAME"].dropna().astype(str).unique().tolist())
        cfg_by_biomarker: dict[str, RegressionConfig] = {}
        for tn in biomarkers:
            cfg_by_biomarker[tn] = _effective_config(
                project_id=int(project_id),
                testname=str(tn),
                global_cfg=global_cfg,
                project_cfgs=project_cfgs,
                test_cfgs=test_cfgs,
            )

        # Partition biomarkers by effective config, so we can apply preprocessing chunk-wise.
        groups: dict[RegressionConfig, list[str]] = {}
        for tn, cfg in cfg_by_biomarker.items():
            groups.setdefault(cfg, []).append(tn)

        omnibus_chunks: list[pd.DataFrame] = []
        pairwise_chunks: list[pd.DataFrame] = []

        for cfg, tns in groups.items():
            chunk = sub.loc[sub["TESTNAME"].astype(str).isin(set(tns))].copy()
            chunk = _apply_pd_only_filter(chunk, pd_only=cfg.pd_only)
            chunk = _apply_gba_filter(chunk, gba_included=cfg.gba_included)
            chunk = _apply_log_transform(chunk, log_transform=cfg.log_transform)
            if chunk.empty:
                continue

            testvalue_col = "LOG_TESTVALUE" if cfg.log_transform else "TESTVALUE"
            z_col = "LOG_TESTVALUE_Z" if cfg.log_transform else "TESTVALUE_Z"

            try:
                omnibus_df_c, pairwise_df_c = run_biomarker_by_biomarker_cohort_regressions(
                    chunk,
                    standardize_within_biomarker=True,
                    standardize_outcome_for_beta=True,
                    cohort_col=cohort_col,
                    testvalue_col=testvalue_col,
                    z_col=z_col,
                    add_conf_int=False,
                )
            except ValueError as exc:
                print(f"PROJECTID={project_id}: skipping config chunk {cfg} due to: {exc}")
                continue

            omnibus_df_c["gba_included"] = bool(cfg.gba_included)
            omnibus_df_c["log_transform"] = bool(cfg.log_transform)
            omnibus_df_c["pd_only"] = bool(cfg.pd_only)
            if not pairwise_df_c.empty:
                pairwise_df_c["gba_included"] = bool(cfg.gba_included)
                pairwise_df_c["log_transform"] = bool(cfg.log_transform)
                pairwise_df_c["pd_only"] = bool(cfg.pd_only)

            omnibus_chunks.append(omnibus_df_c)
            pairwise_chunks.append(pairwise_df_c)

        if not omnibus_chunks:
            print(f"Skipping PROJECTID={project_id}: no config chunk produced output after filtering.")
            continue

        omnibus_df_p = pd.concat(omnibus_chunks, ignore_index=True)
        pairwise_df_p = pd.concat(pairwise_chunks, ignore_index=True) if pairwise_chunks else pd.DataFrame()

        # Recompute BH-FDR across *all* biomarkers/pairwise rows within the project.
        if "omnibus_pval" in omnibus_df_p.columns:
            omnibus_df_p["omnibus_qval_fdr_bh"] = _bh_fdr(omnibus_df_p["omnibus_pval"])
        if not pairwise_df_p.empty and "pval" in pairwise_df_p.columns:
            pairwise_df_p["qval_fdr_bh"] = _bh_fdr(pd.to_numeric(pairwise_df_p["pval"], errors="coerce"))

        omnibus_df_p.insert(0, "PROJECTID", project_id)
        pairwise_df_p.insert(0, "PROJECTID", project_id)
        omnibus_parts.append(omnibus_df_p)
        pairwise_parts.append(pairwise_df_p)
        print(
            f"PROJECTID={project_id}: biomarkers n={len(omnibus_df_p)}, "
            f"pairwise rows n={len(pairwise_df_p)}"
        )

    if not omnibus_parts:
        raise RuntimeError(
            "No projects produced regression output. "
            "Check that each PROJECTID has at least two COHORT levels after filtering."
        )

    omnibus_df = pd.concat(omnibus_parts, ignore_index=True)
    pairwise_df = pd.concat(pairwise_parts, ignore_index=True)

    omnibus_path = output_dir / "biomarker_cohort_omnibus.csv"
    pairwise_path = output_dir / "biomarker_cohort_pairwise.csv"

    existing_omnibus = _read_if_exists(omnibus_path)
    existing_pairwise = _read_if_exists(pairwise_path)

    if not existing_omnibus.empty:
        omnibus_df = pd.concat([existing_omnibus, omnibus_df], ignore_index=True)
    if not existing_pairwise.empty:
        pairwise_df = pd.concat([existing_pairwise, pairwise_df], ignore_index=True)

    # De-duplicate across repeated runs.
    if not omnibus_df.empty:
        for c in ("TESTNAME",):
            if c not in omnibus_df.columns:
                raise RuntimeError(f"Expected {c!r} in omnibus output for de-duplication.")
        omnibus_df = omnibus_df.drop_duplicates(subset=["TESTNAME"], keep="last")

    if not pairwise_df.empty:
        for c in ("TESTNAME", "comparison"):
            if c not in pairwise_df.columns:
                raise RuntimeError(f"Expected {c!r} in pairwise output for de-duplication.")
        pairwise_df = pairwise_df.drop_duplicates(
            subset=["TESTNAME", "comparison"], keep="last"
        )

    omnibus_df.to_csv(omnibus_path, index=False)
    pairwise_df.to_csv(pairwise_path, index=False)

    print(f"Wrote:\n- {omnibus_path}\n- {pairwise_path}")


if __name__ == "__main__":
    main()
