from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from utils.outliers import drop_outlier_rows

try:
    import patsy
    import statsmodels.formula.api as smf
except ModuleNotFoundError as e:
    # This module is importable even when statsmodels isn't installed yet; runtime
    # usage will raise a clearer error from functions that need statsmodels.
    patsy = None  # type: ignore[assignment]
    smf = None  # type: ignore[assignment]
    _IMPORT_ERROR = e
else:
    _IMPORT_ERROR = None


@dataclass(frozen=True)
class PairwiseResult:
    testname: str
    comparison: str
    n: float
    beta: float
    pval: float
    effect_size_std: float
    units: Optional[str] = None
    mean_diff_raw: Optional[float] = None
    beta_ci_low: Optional[float] = None
    beta_ci_high: Optional[float] = None
    effect_size_std_ci_low: Optional[float] = None
    effect_size_std_ci_high: Optional[float] = None


def _require_statsmodels() -> None:
    if _IMPORT_ERROR is not None:
        raise ModuleNotFoundError(
            "statsmodels (and patsy) are required to run the regression pipeline. "
            "Install with: pip install statsmodels"
        ) from _IMPORT_ERROR


def preprocess_biomarker_long_format(
    df: pd.DataFrame,
    *,
    standardize_within_biomarker: bool = True,
    cohort_categories: Optional[Sequence[str]] = None,
    sex_categories: Optional[Sequence[str]] = None,
    testname_col: str = "TESTNAME",
    testvalue_col: str = "TESTVALUE",
    cohort_col: str = "HEURISTIC",
    sex_col: str = "SEX",
    age_col: str = "AGE_AT_VISIT",
    z_col: str = "TESTVALUE_Z",
) -> pd.DataFrame:
    """
    Preprocess long-format biomarker data for regression.

    Each row is one subject-biomarker measurement.
    """
    dfc = df.copy(deep=True)

    required = [testvalue_col, cohort_col, sex_col, testname_col, age_col]
    missing_required = [c for c in required if c not in dfc.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    # Drop rows with missing biomarker outcome or grouping variables.
    dfc = dfc.dropna(subset=required)

    # Coerce TESTVALUE to numeric; drop rows that can't be converted.
    dfc[testvalue_col] = pd.to_numeric(dfc[testvalue_col], errors="coerce")
    dfc = dfc.dropna(subset=[testvalue_col])

    dfc[age_col] = pd.to_numeric(dfc[age_col], errors="coerce")
    dfc = dfc.dropna(subset=[age_col])

    # Make grouping variables categorical for statsmodels C().
    if cohort_categories is None:
        cohort_levels = sorted(dfc[cohort_col].dropna().unique().tolist())
    else:
        cohort_levels = list(cohort_categories)
        # Drop unexpected levels (keeps C() encoding stable when categories are passed in).
        dfc = dfc[dfc[cohort_col].isin(set(cohort_levels))]

    if sex_categories is None:
        sex_levels = sorted(dfc[sex_col].dropna().unique().tolist())
    else:
        sex_levels = list(sex_categories)
        dfc = dfc[dfc[sex_col].isin(set(sex_levels))]

    dfc[cohort_col] = pd.Categorical(dfc[cohort_col], categories=cohort_levels)
    dfc[sex_col] = pd.Categorical(dfc[sex_col], categories=sex_levels)

    if standardize_within_biomarker:
        means = dfc.groupby(testname_col, observed=False)[testvalue_col].transform("mean")
        stds = dfc.groupby(testname_col, observed=False)[testvalue_col].transform("std")
        # Avoid divide-by-zero; if std=0, z-score is treated as 0.
        stds_safe = stds.replace(0.0, np.nan)
        dfc[z_col] = (dfc[testvalue_col] - means) / stds_safe
        dfc[z_col] = dfc[z_col].fillna(0.0)

    return dfc


def fit_ols_model_for_biomarker(
    df: pd.DataFrame,
    biomarker: str,
    *,
    standardize_outcome: bool,
    testname_col: str = "TESTNAME",
    testvalue_col: str = "TESTVALUE",
    cohort_col: str = "HEURISTIC",
    sex_col: str = "SEX",
    age_col: str = "AGE_AT_VISIT",
    z_col: str = "TESTVALUE_Z",
) -> object:
    """
    Fit one OLS model for a single biomarker.

    Uses:
      - standardized outcome: `TESTVALUE_Z ~ C(group) + C(SEX) + AGE_AT_VISIT`
      - raw outcome:         `TESTVALUE ~ C(group) + C(SEX) + AGE_AT_VISIT`
    where `group` is `cohort_col` (e.g. COHORT or HEURISTIC).
    """
    outcome_var = z_col if standardize_outcome else testvalue_col
    if standardize_outcome and z_col not in df.columns:
        raise ValueError(f"standardize_outcome=True but {z_col!r} is not present in df.")
    if age_col not in df.columns:
        raise ValueError(f"Missing required column {age_col!r}.")

    return _fit_ols_for_biomarker(
        df=df,
        biomarker=biomarker,
        outcome_var=outcome_var,
        testname_col=testname_col,
        cohort_col=cohort_col,
        sex_col=sex_col,
        age_col=age_col,
    )


def _build_formula(
    *,
    outcome_var: str,
    cohort_col: str,
    sex_col: str,
    age_col: str,
) -> str:
    return f"{outcome_var} ~ C({cohort_col}) + C({sex_col}) + {age_col}"


def _fit_ols_for_biomarker(
    *,
    df: pd.DataFrame,
    biomarker: str,
    outcome_var: str,
    testname_col: str,
    cohort_col: str,
    sex_col: str,
    age_col: str,
) -> "smf.ols":
    _require_statsmodels()
    assert smf is not None  # for type checkers

    dfb = df.loc[df[testname_col] == biomarker].copy()

    # Restrict categorical levels to those observed for this biomarker to avoid
    # rank-deficiency when a cohort/sex is absent.
    cohort_present = [c for c in dfb[cohort_col].cat.categories if c in set(dfb[cohort_col].dropna().unique())]
    sex_present = [s for s in dfb[sex_col].cat.categories if s in set(dfb[sex_col].dropna().unique())]
    if len(cohort_present) < 2:
        raise ValueError(f"Need >=2 cohort groups for biomarker={biomarker}; got {cohort_present}")
    if len(sex_present) < 1:
        raise ValueError(f"No SEX groups for biomarker={biomarker}")

    dfb[cohort_col] = pd.Categorical(dfb[cohort_col], categories=cohort_present)
    dfb[sex_col] = pd.Categorical(dfb[sex_col], categories=sex_present)

    formula = _build_formula(
        outcome_var=outcome_var,
        cohort_col=cohort_col,
        sex_col=sex_col,
        age_col=age_col,
    )

    model = smf.ols(formula=formula, data=dfb, missing="drop")
    res = model.fit()
    return res


def _get_design_info(res: object, *, cohort_col: str, sex_col: str) -> tuple[object, object, object]:
    # statsmodels' internals have changed across versions; use duck-typing.
    model = getattr(res, "model")
    data_frame = getattr(getattr(model, "data"), "frame")
    design_info = getattr(getattr(model, "data"), "design_info")
    assert design_info is not None
    assert data_frame is not None
    return model, data_frame, design_info


def _build_contrast_vector(
    *,
    res: object,
    cohort_a: str,
    cohort_b: str,
    sex_value: str,
    age_value: float,
    testname_col: str,
    cohort_col: str,
    sex_col: str,
    age_col: str,
) -> np.ndarray:
    _require_statsmodels()
    assert patsy is not None  # for type checkers

    _, train_frame, design_info = _get_design_info(res, cohort_col=cohort_col, sex_col=sex_col)

    # design_info already contains how to encode categorical levels from the fit.
    cohort_cats = train_frame[cohort_col].cat.categories
    sex_cats = train_frame[sex_col].cat.categories

    def _make_row(cohort_value: str) -> pd.DataFrame:
        row = {
            cohort_col: pd.Categorical([cohort_value], categories=cohort_cats),
            sex_col: pd.Categorical([sex_value], categories=sex_cats),
        }
        row[age_col] = [age_value]
        # TESTNAME isn't used by the formula, but including it makes the row
        # consistent with the design_info's expected frame columns.
        row[testname_col] = ["__unused__"]
        return pd.DataFrame(row)

    row_a = _make_row(cohort_a)
    row_b = _make_row(cohort_b)

    dm_a = patsy.build_design_matrices([design_info], row_a, return_type="matrix")[0]
    dm_b = patsy.build_design_matrices([design_info], row_b, return_type="matrix")[0]
    exog_a = np.asarray(dm_a).reshape(-1)
    exog_b = np.asarray(dm_b).reshape(-1)
    return exog_a - exog_b


def _safe_t_test(
    *,
    res: object,
    contrast: np.ndarray,
    alpha: float,
    compute_ci: bool,
) -> tuple[float, float, Optional[float], Optional[float]]:
    ttest_res = getattr(res, "t_test")(contrast.reshape(1, -1))
    effect = float(np.asarray(ttest_res.effect).reshape(-1)[0])
    pval = float(np.asarray(ttest_res.pvalue).reshape(-1)[0])
    ci_low = None
    ci_high = None
    if compute_ci:
        try:
            ci = np.asarray(ttest_res.conf_int(alpha=alpha)).reshape(2)
            ci_low = float(ci[0])
            ci_high = float(ci[1])
        except Exception:
            pass
    return effect, pval, ci_low, ci_high


def _omnibus_cohort_pval(
    *,
    res: object,
    cohort_col: str,
) -> float:
    params = getattr(res, "params")
    param_names: list[str] = list(getattr(params, "index"))
    cohort_param_names = [n for n in param_names if f"C({cohort_col})" in n]
    if len(cohort_param_names) < 1:
        return float("nan")

    idx = {name: i for i, name in enumerate(param_names)}
    R = np.zeros((len(cohort_param_names), len(param_names)))
    for r_i, p_name in enumerate(cohort_param_names):
        R[r_i, idx[p_name]] = 1.0

    ftest_res = getattr(res, "f_test")(R)
    return float(np.asarray(ftest_res.pvalue).reshape(-1)[0])


def _bh_fdr(pvals: pd.Series) -> pd.Series:
    """
    Benjamini-Hochberg FDR correction.

    NaNs are preserved as NaNs.
    """
    p = pvals.astype(float).copy()
    out = pd.Series(np.nan, index=p.index, dtype=float)

    mask = p.notna()
    if mask.sum() == 0:
        return out

    p_nonan = p.loc[mask].sort_values()
    m = len(p_nonan)
    ranks = np.arange(1, m + 1, dtype=float)
    adj = p_nonan.values * m / ranks
    # Enforce monotonicity.
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    out.loc[p_nonan.index] = np.clip(adj, 0.0, 1.0)
    return out


def run_biomarker_by_biomarker_cohort_regressions(
    df: pd.DataFrame,
    *,
    standardize_within_biomarker: bool = True,
    standardize_outcome_for_beta: bool = False,
    cohort_categories: Optional[Sequence[str]] = None,
    testname_col: str = "TESTNAME",
    testvalue_col: str = "TESTVALUE",
    cohort_col: str = "HEURISTIC",
    sex_col: str = "SEX",
    age_col: str = "AGE_AT_VISIT",
    z_col: str = "TESTVALUE_Z",
    units_col: str = "UNITS",
    alpha: float = 0.05,
    add_conf_int: bool = False,
    include_raw_mean_diff: bool = True,
    outlier_handling: str = "none",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run per-biomarker OLS regressions.

    With defaults (standardize_within_biomarker=True, standardize_outcome_for_beta=False):
      - Main model: raw outcome ~ C(cohort_col) + C(SEX) + AGE_AT_VISIT
        drives pairwise ``beta``, ``pval``, and the omnibus cohort test.
      - Parallel model on ``z_col`` (within-biomarker z-scores) with the same
        predictors drives ``effect_size_std``.

    If ``standardize_outcome_for_beta=True``, the main model uses ``z_col`` instead,
    so ``beta`` matches ``effect_size_std`` (aside from numerical noise).

    Returns:
      - omnibus_df: one row per biomarker (omnibus cohort p-value + BH q-value)
      - pairwise_df: one row per biomarker x cohort-pair (t-tests via model contrasts)
    """
    df_clean = drop_outlier_rows(
        df,
        method=outlier_handling,
        value_col=testvalue_col,
        group_col=testname_col,
    )
    dfp = preprocess_biomarker_long_format(
        df_clean,
        standardize_within_biomarker=standardize_within_biomarker,
        cohort_categories=cohort_categories,
        testname_col=testname_col,
        testvalue_col=testvalue_col,
        cohort_col=cohort_col,
        sex_col=sex_col,
        age_col=age_col,
        z_col=z_col,
    )

    if z_col not in dfp.columns and standardize_within_biomarker:
        raise RuntimeError(f"Expected standardized column {z_col} to be created.")

    cohort_levels: list[str] = list(dfp[cohort_col].cat.categories)
    sex_levels: list[str] = list(dfp[sex_col].cat.categories)
    if len(cohort_levels) < 2:
        raise ValueError(
            f"Need >=2 cohort groups for cohort contrasts in {cohort_col}, got {len(cohort_levels)}"
        )

    omnibus_rows: list[dict[str, object]] = []
    pairwise_rows: list[PairwiseResult] = []

    _use_units = units_col in dfp.columns
    if _use_units:
        _pairs = (
            dfp[[testname_col, units_col]]
            .dropna(subset=[testname_col])
            .drop_duplicates()
        )
        biomarker_levels = list(_pairs.itertuples(index=False, name=None))
    else:
        biomarker_levels = [(b, None) for b in dfp[testname_col].dropna().unique()]

    for biomarker, units_val in biomarker_levels:
        if _use_units and units_val is not None:
            dfb = dfp.loc[
                (dfp[testname_col] == biomarker) & (dfp[units_col] == units_val)
            ].copy()
        else:
            dfb = dfp.loc[dfp[testname_col] == biomarker].copy()

        # Recast for contrasts; the model fit will further restrict levels.
        present_cohorts = [c for c in cohort_levels if c in set(dfb[cohort_col].dropna().unique())]
        present_sexes = [s for s in sex_levels if s in set(dfb[sex_col].dropna().unique())]
        if len(present_cohorts) < 2:
            omnibus_rows.append(
                {
                    "cohort_col": cohort_col,
                    testname_col: biomarker,
                    units_col: units_val,
                    "n": float(len(dfb)),
                    "omnibus_pval": float("nan"),
                }
            )
            # Still emit pairwise rows for all 3 cohort pairs with NaNs.
            for a, b in itertools.combinations(cohort_levels, 2):
                pairwise_rows.append(
                    PairwiseResult(
                        testname=biomarker,
                        units=units_val,
                        comparison=f"{a} vs {b}",
                        n=float("nan"),
                        beta=float("nan"),
                        pval=float("nan"),
                        effect_size_std=float("nan"),
                        mean_diff_raw=(None if not include_raw_mean_diff else float("nan")),
                    )
                )
            continue

        dfb[cohort_col] = pd.Categorical(dfb[cohort_col], categories=present_cohorts)
        dfb[sex_col] = pd.Categorical(dfb[sex_col], categories=present_sexes)

        # Use the first observed sex as reference for contrasts; due to additive structure
        # (no cohort*sex interaction), cohort differences don't depend on reference values.
        sex_ref = dfb[sex_col].cat.categories[0]
        age_value = float(dfb[age_col].median())

        # Fit standardized outcome model for effect_size_std output.
        try:
            res_std = _fit_ols_for_biomarker(
                df=dfb,
                biomarker=biomarker,
                outcome_var=z_col,
                testname_col=testname_col,
                cohort_col=cohort_col,
                sex_col=sex_col,
                age_col=age_col,
            )
        except Exception:
            res_std = None

        # Fit the "main" model whose coefficients/p-values are used for beta/pval.
        outcome_main = z_col if standardize_outcome_for_beta else testvalue_col
        try:
            res_main = _fit_ols_for_biomarker(
                df=dfb,
                biomarker=biomarker,
                outcome_var=outcome_main,
                testname_col=testname_col,
                cohort_col=cohort_col,
                sex_col=sex_col,
                age_col=age_col,
            )
        except Exception:
            res_main = None

        # Omnibus cohort test.
        if res_main is not None:
            omnibus_p = _omnibus_cohort_pval(res=res_main, cohort_col=cohort_col)
            n_omnibus = float(getattr(res_main, "nobs", np.nan))
        else:
            omnibus_p = float("nan")
            n_omnibus = float(len(dfb))
        omnibus_rows.append(
            {
                "cohort_col": cohort_col,
                testname_col: biomarker,
                units_col: units_val,
                "n": n_omnibus,
                "omnibus_pval": omnibus_p,
            }
        )

        # Cohort pairwise comparisons (all 3 global pairs).
        raw_means = None
        if include_raw_mean_diff:
            raw_means = (
                dfb.groupby(cohort_col, observed=False)[testvalue_col].mean().to_dict()
            )

        for a, b in itertools.combinations(cohort_levels, 2):
            comparison_label = f"{a} vs {b}"
            if a not in present_cohorts or b not in present_cohorts or res_main is None:
                pairwise_rows.append(
                    PairwiseResult(
                        testname=biomarker,
                        units=units_val,
                        comparison=comparison_label,
                        n=float("nan"),
                        beta=float("nan"),
                        pval=float("nan"),
                        effect_size_std=float("nan"),
                        mean_diff_raw=(None if not include_raw_mean_diff else float("nan")),
                    )
                )
                continue

            contrast_main = _build_contrast_vector(
                res=res_main,
                cohort_a=a,
                cohort_b=b,
                sex_value=sex_ref,
                age_value=age_value,
                testname_col=testname_col,
                cohort_col=cohort_col,
                sex_col=sex_col,
                age_col=age_col,
            )
            beta, pval, beta_ci_low, beta_ci_high = _safe_t_test(
                res=res_main,
                contrast=contrast_main,
                alpha=alpha,
                compute_ci=add_conf_int,
            )

            effect_std = float("nan")
            effect_ci_low = None
            effect_ci_high = None
            if res_std is not None:
                contrast_std = _build_contrast_vector(
                    res=res_std,
                    cohort_a=a,
                    cohort_b=b,
                    sex_value=sex_ref,
                    age_value=age_value,
                    testname_col=testname_col,
                    cohort_col=cohort_col,
                    sex_col=sex_col,
                    age_col=age_col,
                )
                effect_std, _, effect_ci_low, effect_ci_high = _safe_t_test(
                    res=res_std,
                    contrast=contrast_std,
                    alpha=alpha,
                    compute_ci=add_conf_int,
                )

            raw_mean_diff = float("nan")
            if raw_means is not None:
                raw_mean_diff = float(raw_means.get(a, np.nan) - raw_means.get(b, np.nan))

            n_pairwise = float(dfb.loc[dfb[cohort_col].isin([a, b])].shape[0])

            pairwise_rows.append(
                PairwiseResult(
                    testname=biomarker,
                    units=units_val,
                    comparison=comparison_label,
                    n=n_pairwise,
                    beta=beta,
                    pval=pval,
                    effect_size_std=effect_std,
                    mean_diff_raw=(None if not include_raw_mean_diff else raw_mean_diff),
                    beta_ci_low=beta_ci_low if add_conf_int else None,
                    beta_ci_high=beta_ci_high if add_conf_int else None,
                    effect_size_std_ci_low=effect_ci_low if add_conf_int else None,
                    effect_size_std_ci_high=effect_ci_high if add_conf_int else None,
                )
            )

    omnibus_df = pd.DataFrame(omnibus_rows)
    omnibus_df["omnibus_qval_fdr_bh"] = _bh_fdr(omnibus_df["omnibus_pval"])

    pairwise_df = pd.DataFrame([r.__dict__ for r in pairwise_rows])
    pairwise_df["comparison"] = pairwise_df["comparison"].astype(str)
    pairwise_df["pval"] = pd.to_numeric(pairwise_df["pval"], errors="coerce")
    pairwise_df["qval_fdr_bh"] = _bh_fdr(pairwise_df["pval"])

    # Rename internal field names to canonical output column names.
    pairwise_df = pairwise_df.rename(columns={"testname": testname_col, "units": units_col})
    omnibus_df = omnibus_df.rename(columns={testname_col: "TESTNAME", units_col: "UNITS"})
    pairwise_df = pairwise_df.rename(columns={testname_col: "TESTNAME", units_col: "UNITS"})

    # Place UNITS immediately after TESTNAME in both outputs.
    for name in ("omnibus_df", "pairwise_df"):
        _df = omnibus_df if name == "omnibus_df" else pairwise_df
        if "UNITS" in _df.columns:
            cols = list(_df.columns)
            cols.remove("UNITS")
            cols.insert(cols.index("TESTNAME") + 1, "UNITS")
            if name == "omnibus_df":
                omnibus_df = _df[cols]
            else:
                pairwise_df = _df[cols]

    if not include_raw_mean_diff and "mean_diff_raw" in pairwise_df.columns:
        pairwise_df = pairwise_df.drop(columns=["mean_diff_raw"])

    omnibus_df["outlier_handling"] = outlier_handling
    pairwise_df["outlier_handling"] = outlier_handling

    return omnibus_df, pairwise_df

