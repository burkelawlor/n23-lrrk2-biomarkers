from __future__ import annotations

import numpy as np
import pandas as pd

_VALID_METHODS = frozenset({"none", "std", "iqr"})


def drop_outlier_rows(
    df: pd.DataFrame,
    *,
    method: str,
    value_col: str,
    group_col: str = "TESTNAME",
) -> pd.DataFrame:
    """
    Drop rows flagged as outliers within each group_col level, using value_col.

    For method \"std\", uses sample standard deviation (pandas default ddof=1).
    """
    if method not in _VALID_METHODS:
        raise ValueError(f"method must be one of {sorted(_VALID_METHODS)}, got {method!r}")
    if method == "none":
        return df

    if value_col not in df.columns:
        raise ValueError(f"Missing value column {value_col!r}.")
    if group_col not in df.columns:
        raise ValueError(f"Missing group column {group_col!r}.")

    dfc = df.copy()
    dfc[value_col] = pd.to_numeric(dfc[value_col], errors="coerce")

    if method == "std":
        keep_mask = pd.Series(True, index=dfc.index)

        for _, grp in dfc.groupby(group_col, observed=False):
            idx = grp.index
            vals = grp[value_col]
            finite = vals[np.isfinite(pd.to_numeric(vals, errors="coerce").to_numpy(dtype=float))]
            if len(finite) == 0:
                keep_mask.loc[idx] = False
                continue
            std = float(finite.std(ddof=1))
            mean = float(finite.mean())
            if not np.isfinite(std) or std == 0.0:
                continue
            z = (vals - mean) / std
            bad = vals.notna() & z.notna() & (z.abs() > 3.0)
            keep_mask.loc[idx] = ~bad

        return dfc.loc[keep_mask].copy()

    if method == "iqr":
        keep_mask = pd.Series(True, index=dfc.index)

        for _, grp in dfc.groupby(group_col, observed=False):
            idx = grp.index
            vals = grp[value_col]
            finite = vals[np.isfinite(pd.to_numeric(vals, errors="coerce").to_numpy(dtype=float))]
            if len(finite) == 0:
                keep_mask.loc[idx] = False
                continue
            q1 = float(finite.quantile(0.25))
            q3 = float(finite.quantile(0.75))
            iqr = q3 - q1
            if not np.isfinite(iqr) or iqr == 0.0:
                continue
            low = q1 - 1.5 * iqr
            high = q3 + 1.5 * iqr
            bad = vals.notna() & ((vals < low) | (vals > high))
            keep_mask.loc[idx] = ~bad

        return dfc.loc[keep_mask].copy()

    raise ValueError(f"Unknown outlier method: {method!r}")
