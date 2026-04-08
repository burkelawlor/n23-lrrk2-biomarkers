"""Helpers for merging cleaned biospecimen tables into a durable CSV."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Sequence

import pandas as pd

# Local DB helpers live in utils/db.py; imported lazily in the append function.

# Long-form biospecimen + ML flags. PI fields live in cleaned_biospecimen_projects.csv.
_CANONICAL_CLEANED_BIOSPECIMEN_COLUMNS: tuple[str, ...] = (
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
    # "update_stamp",
    "RV",
    "GBA",
    "PREDICTED",
    "DRIVEN",
    "HEURISTIC",
)

_PROJECT_METADATA_COLUMNS: tuple[str, ...] = ("PROJECTID", "PI_NAME", "PI_INSTITUTION")

_DEFAULT_DEDUPE_SUBSET: tuple[str, ...] = (
    "PATNO",
    "PROJECTID",
    "TESTNAME",
    "CLINICAL_EVENT",
    "TYPE",
    "RUNDATE",
)

_REPO_ROOT = Path(__file__).resolve().parent.parent

_DEFAULT_OUTPUT_PATH = _REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_analysis.csv"

_DEFAULT_PROJECT_METADATA_PATH = _REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv"


def _canonical_biospecimen_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only ``_CANONICAL_CLEANED_BIOSPECIMEN_COLUMNS``; missing columns become NA."""
    return df.reindex(columns=list(_CANONICAL_CLEANED_BIOSPECIMEN_COLUMNS))


def _strip_pi_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop = [c for c in ("PI_NAME", "PI_INSTITUTION") if c in df.columns]
    return df.drop(columns=drop) if drop else df


def _extract_project_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """One row per PROJECTID from frames that still carry PI columns."""
    if df.empty or "PROJECTID" not in df.columns:
        return pd.DataFrame(columns=list(_PROJECT_METADATA_COLUMNS))
    if "PI_NAME" not in df.columns and "PI_INSTITUTION" not in df.columns:
        return pd.DataFrame(columns=list(_PROJECT_METADATA_COLUMNS))
    sub = df.loc[:, [c for c in _PROJECT_METADATA_COLUMNS if c in df.columns]].copy()
    for c in _PROJECT_METADATA_COLUMNS:
        if c not in sub.columns:
            sub[c] = pd.NA
    sub = sub[list(_PROJECT_METADATA_COLUMNS)]
    sub = sub.dropna(subset=["PROJECTID"])
    return sub.drop_duplicates(subset=["PROJECTID"], keep="first")


def _merge_project_metadata(
    incoming_meta: pd.DataFrame,
    path: Path,
    *,
    keep: Literal["first", "last"],
) -> pd.DataFrame:
    if path.exists():
        existing = pd.read_csv(path, low_memory=False)
        combined = pd.concat([existing, incoming_meta], ignore_index=True)
    else:
        combined = incoming_meta.reset_index(drop=True)

    combined = combined.drop_duplicates(subset=["PROJECTID"], keep=keep)
    return combined.sort_values("PROJECTID", kind="mergesort").reset_index(drop=True)


def _atomic_to_csv(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def append_to_cleaned_biospecimen_csv(
    df: pd.DataFrame,
    *,
    output_path: Path | str | None = None,
    project_metadata_path: Path | str | None = None,
    dedupe_subset: Sequence[str] | None = None,
    keep: Literal["first", "last"] = "first",
) -> pd.DataFrame:
    """
    Merge ``df`` into a single cleaned-biospecimen CSV without clobbering prior rows.

    Row-level data are written to ``cleaned_biospecimen_analysis.csv`` (or
    ``output_path``). ``PI_NAME`` and ``PI_INSTITUTION`` are not stored there;
    they are merged into ``cleaned_biospecimen_projects.csv`` (or
    ``project_metadata_path``) with one row per ``PROJECTID``.

    If the incoming frame includes PI columns, project metadata is updated; if those
    columns are absent, the project-level file is left unchanged (except it is still
    read when present so callers can rely on it existing after prior runs).

    If ``output_path`` does not exist, it is created (including parent directories).
    If it exists, existing rows are loaded, concatenated with ``df``, de-duplicated,
    and written back. Duplicates are defined by ``dedupe_subset`` (same semantics as
    :meth:`pandas.DataFrame.drop_duplicates`).

    With the default ``keep='first'``, rows already on disk win over duplicate rows
    in ``df``. Use ``keep='last'`` to prefer newly supplied rows.

    The written CSV (and returned dataframe) contain only
    ``_CANONICAL_CLEANED_BIOSPECIMEN_COLUMNS``; any other columns on ``df`` or on disk
    are dropped. Missing canonical columns are filled with NA.

    This function is intentionally file-based. Database ingestion (local or hosted)
    is handled by scripts under ``scripts/`` so both ingestion methods share the
    same code path.
    """
    out = Path(output_path) if output_path is not None else _DEFAULT_OUTPUT_PATH
    proj_path = (
        Path(project_metadata_path)
        if project_metadata_path is not None
        else _DEFAULT_PROJECT_METADATA_PATH
    )

    if df.empty:
        if out.exists():
            return _canonical_biospecimen_columns(
                _strip_pi_columns(pd.read_csv(out, low_memory=False))
            )
        return pd.DataFrame(columns=list(_CANONICAL_CLEANED_BIOSPECIMEN_COLUMNS))

    keys = tuple(dedupe_subset) if dedupe_subset is not None else _DEFAULT_DEDUPE_SUBSET
    missing_keys = [k for k in keys if k not in df.columns]
    if missing_keys:
        raise ValueError(
            "Incoming dataframe is missing dedupe columns "
            f"{missing_keys!r}; present columns: {list(df.columns)!r}"
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    incoming = df.drop_duplicates(subset=list(keys), keep=keep).copy()
    incoming_meta = _extract_project_metadata(incoming)

    if out.exists():
        existing = pd.read_csv(out, low_memory=False)
        existing = _strip_pi_columns(existing)
        e_missing = [k for k in keys if k not in existing.columns]
        if e_missing:
            raise ValueError(
                f"Existing file {out} is missing dedupe columns {e_missing!r}; "
                f"found columns: {list(existing.columns)!r}"
            )
        incoming = _strip_pi_columns(incoming)
        combined = pd.concat([existing, incoming], ignore_index=True)
    else:
        combined = _strip_pi_columns(incoming.reset_index(drop=True))

    combined = combined.drop_duplicates(subset=list(keys), keep=keep)
    combined = _canonical_biospecimen_columns(combined)

    _atomic_to_csv(combined, out)

    if not incoming_meta.empty:
        proj_path.parent.mkdir(parents=True, exist_ok=True)
        projects = _merge_project_metadata(incoming_meta, proj_path, keep=keep)
        _atomic_to_csv(projects, proj_path)

    return combined
