from __future__ import annotations

from pathlib import Path
import logging
import time
import urllib.parse
from contextlib import contextmanager
from io import StringIO
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from astropy.stats import freedman_bin_width
from dash import Input, Output, State, callback, dcc, dash_table, html
import dash
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from sqlalchemy import text

from utils.cache_runtime import memoize
from utils.biomarker_regression import run_biomarker_by_biomarker_cohort_regressions
from utils.db_runtime import (
    fetch_analysis_subset,
    get_engine_from_env,
    get_project_rundates_for_project,
    get_projects_lookup,
    get_testnames,
)
from utils.outliers import drop_outlier_rows
from utils.regression_config import effective_config, load_regression_configs

_LOG = logging.getLogger(__name__)


@contextmanager
def _timed(label: str, **fields: Any):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        extra = " ".join([f"{k}={v}" for k, v in fields.items() if v is not None])
        _LOG.info("perf %s %.3fs %s", label, dt, extra)


dash.register_page(
    __name__,
    path="/analysis",
    name="Biomarker Analysis",
    title="Biomarker Analysis | Biomarker Dashboard",
)


_ENGINE = None

COHORTS: dict[str, dict[str, object]] = {
    "COHORT": {
        "Order": ["Control", "Prodromal", "PD"],
        "Colors": {
            "Control": "#1f77b4",
            "Prodromal": "#ff7f0e",
            "PD": "#d62728",
        },
    },
    "HEURISTIC": {
        "Order": ["Non", "Predicted", "RV"],
        "Colors": {
            "Non": "#F7794F",
            "Predicted": "#739898",
            "RV": "#024C4C",
        },
    },
}


def _normalize_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {
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
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    keep_cols = [
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
    # Optional metadata columns that we want to preserve downstream (eg. for downloads).
    for optional in ["CLINICAL_EVENT"]:
        if optional in df.columns and optional not in keep_cols:
            keep_cols.append(optional)

    out = df.loc[:, keep_cols].copy()

    out["TESTVALUE"] = pd.to_numeric(out["TESTVALUE"], errors="coerce")
    out["GBA"] = pd.to_numeric(out["GBA"], errors="coerce")
    out["AGE_AT_VISIT"] = pd.to_numeric(out["AGE_AT_VISIT"], errors="coerce")
    out["PROJECTID"] = out["PROJECTID"].astype(str).where(out["PROJECTID"].notna(), other=None)
    out["RUNDATE"] = pd.to_datetime(out["RUNDATE"], errors="coerce")
    out = out.dropna(
        subset=[
            "TESTNAME",
            "COHORT",
            "HEURISTIC",
            "GBA",
            "TESTVALUE",
            "SEX",
            "PATNO",
            "PROJECTID",
            "RUNDATE",
            "AGE_AT_VISIT",
        ]
    )

    out["COHORT"] = out["COHORT"].astype(str)
    out["HEURISTIC"] = out["HEURISTIC"].astype(str)
    out["TESTNAME"] = out["TESTNAME"].astype(str)
    out["SEX"] = out["SEX"].astype(str)
    return out


def _engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = get_engine_from_env()
    return _ENGINE

_REGRESSION_CONFIG_PATH = Path(__file__).resolve().parents[1] / "regression_configs.yaml"
_GLOBAL_CFG, _PROJECT_CFGS, _TEST_CFGS = load_regression_configs(_REGRESSION_CONFIG_PATH)

def _get_grouped_testname_select_data() -> tuple[list[dict[str, object]], list[str]]:
    """
    Returns (mantine_data, flat_testnames).

    Mantine grouped data looks like:
      [{"group": "Project 145", "items": [{"value": "...", "label": "..."}, ...]}, ...]
    """
    try:
        q = """
        SELECT DISTINCT PROJECTID, TESTNAME
        FROM analysis
        WHERE TESTNAME IS NOT NULL
        ORDER BY PROJECTID, TESTNAME
        """
        with _timed("testname_select.grouped_options.query"):
            df = pd.read_sql_query(text(q), _engine())
    except Exception:
        # Fallback to the existing helper if the grouped query fails (eg. missing table).
        tns = get_testnames(_engine())
        return (
            [
                {
                    "group": "Biomarkers",
                    "items": [{"label": t, "value": t} for t in tns],
                }
            ]
            if tns
            else [],
            tns,
        )

    if df.empty or "TESTNAME" not in df.columns:
        return ([], [])

    df = df.copy()
    df["TESTNAME"] = df["TESTNAME"].astype(str)
    df["PROJECTID"] = df["PROJECTID"].astype(str).where(df["PROJECTID"].notna(), other=None)
    # Identify testnames that appear in more than one project so we can make the
    # option values unique for Mantine (it does not allow duplicate values).
    per_testname_project_counts = (
        df.loc[df["PROJECTID"].notna(), ["TESTNAME", "PROJECTID"]]
        .drop_duplicates()
        .groupby("TESTNAME")["PROJECTID"]
        .nunique()
    )
    duplicated_testnames = set(per_testname_project_counts[per_testname_project_counts > 1].index)

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in df.itertuples(index=False):
        group_label = (
            f"Project {row.PROJECTID}" if row.PROJECTID is not None else "Project (Unknown)"
        )
        testname = str(row.TESTNAME)
        project_id = row.PROJECTID

        if testname in duplicated_testnames and project_id is not None:
            value = f"{testname}||{project_id}"
            label = f"{testname} (project {project_id})"
        else:
            value = testname
            label = testname

        grouped.setdefault(group_label, []).append({"label": label, "value": value})

    mantine_data: list[dict[str, object]] = [
        {"group": group_label, "items": items} for group_label, items in grouped.items()
    ]
    # Use the option values (which may be composite) for default selection.
    testnames: list[str] = [item["value"] for group in mantine_data for item in group["items"]]  # type: ignore[index]
    return mantine_data, testnames


def _parse_testname_select_value(value: str | None) -> tuple[str | None, str | None]:
    """
    The biomarker select value is either:
      - "<TESTNAME>" (unique across projects), or
      - "<TESTNAME>||<PROJECTID>" (only when the TESTNAME is duplicated).
    """
    if not value:
        return None, None
    s = str(value)
    if "||" not in s:
        return s, None
    left, right = s.rsplit("||", 1)
    left = left.strip()
    pid = right.strip() or None
    return (left or None), pid


TESTNAME_SELECT_DATA, TESTNAMES = _get_grouped_testname_select_data()
DEFAULT_TESTNAME = TESTNAMES[0] if TESTNAMES else None
try:
    _cohort_df = pd.read_sql_query("SELECT DISTINCT COHORT FROM analysis", _engine())
    _present_cohorts = set(_cohort_df["COHORT"].dropna().astype(str).tolist())
except Exception:
    _present_cohorts = set()
COHORT_VALUES = [c for c in COHORTS["COHORT"]["Order"] if c in _present_cohorts]


def _modal_project_id_for_testname(testname: str) -> str | None:
    try:
        q = """
        SELECT PROJECTID, COUNT(*) AS n
        FROM analysis
        WHERE TESTNAME = :testname AND PROJECTID IS NOT NULL
        GROUP BY PROJECTID
        ORDER BY n DESC
        LIMIT 1
        """
        with _timed("header.modal_project_id.query", testname=str(testname)):
            pid_df = pd.read_sql_query(text(q), _engine(), params={"testname": str(testname)})
    except Exception:
        return None

    if pid_df.empty or "PROJECTID" not in pid_df.columns:
        return None
    val = pid_df["PROJECTID"].iloc[0]
    return str(val) if val is not None else None


def _transform_radio_value_for_testname(testname_select_value: str | None) -> str:
    testname, project_id = _parse_testname_select_value(testname_select_value)
    if not testname:
        return "none"
    cfg = effective_config(
        project_id=project_id if project_id is not None else _modal_project_id_for_testname(str(testname)),
        testname=str(testname),
        global_cfg=_GLOBAL_CFG,
        project_cfgs=_PROJECT_CFGS,
        test_cfgs=_TEST_CFGS,
    )
    return "log" if cfg.log_transform else "none"

DEFAULT_TRANSFORM_VALUE = _transform_radio_value_for_testname(DEFAULT_TESTNAME)


def _outlier_radio_value_for_testname(testname_select_value: str | None) -> str:
    testname, project_id = _parse_testname_select_value(testname_select_value)
    if not testname:
        return "std"
    cfg = effective_config(
        project_id=project_id if project_id is not None else _modal_project_id_for_testname(str(testname)),
        testname=str(testname),
        global_cfg=_GLOBAL_CFG,
        project_cfgs=_PROJECT_CFGS,
        test_cfgs=_TEST_CFGS,
    )
    return cfg.outlier_handling


DEFAULT_OUTLIER_VALUE = _outlier_radio_value_for_testname(DEFAULT_TESTNAME)


def description_card():
    return html.Div(
        id="description-card",
        children=[
            html.Div(
                "Select a biomarker from the dropdown to explore distributions and run statistical comparisons.",
                style={"color": "#6c757d", "fontSize": "13px"},
            )
        ],
    )


def generate_control_card():
    _section_label_style = {
        "fontSize": "10px",
        "fontWeight": "700",
        "letterSpacing": "0.08em",
        "color": "#6c757d",
        "marginBottom": "6px",
        "textTransform": "uppercase",
    }
    _section_divider = html.Hr(style={"margin": "14px 0", "borderColor": "#dee2e6"})

    return html.Div(
        id="control-cards",
        style={
            "backgroundColor": "#f8f9fa",
            "borderRadius": "8px",
            "border": "1px solid #e9ecef",
            "padding": "16px",
        },
        children=[
            # Biomarker select
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("BIOMARKER", style=_section_label_style),
                    (
                        dmc.Select(
                            id="testname",
                            data=TESTNAME_SELECT_DATA,
                            value=DEFAULT_TESTNAME,
                            clearable=False,
                            searchable=True,
                            placeholder="No biomarkers found in TESTNAME",
                            nothingFoundMessage="No biomarkers found",
                            maxDropdownHeight=320,
                        )
                        if dmc is not None
                        else dcc.Dropdown(
                            id="testname",
                            options=[{"label": t, "value": t} for t in TESTNAMES],
                            value=DEFAULT_TESTNAME,
                            clearable=False,
                            placeholder="No biomarkers found in TESTNAME",
                        )
                    ),
                ],
            ),
            _section_divider,
            # Compare groups by
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("COMPARE GROUPS BY", style=_section_label_style),
                    dbc.RadioItems(
                        id="groupby",
                        options=[
                            {"label": "Classifier result", "value": "HEURISTIC"},
                            {"label": "PD Diagnosis", "value": "COHORT"},
                        ],
                        value="HEURISTIC",
                        inline=False,
                    ),
                ],
            ),
            _section_divider,
            # Cohorts to include
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("COHORTS TO INCLUDE", style=_section_label_style),
                    dbc.Checklist(
                        id="cohort_filter",
                        options=[{"label": c, "value": c} for c in COHORT_VALUES],
                        value=COHORT_VALUES,
                        inline=False,
                    ),
                ],
            ),
            _section_divider,
            # GBA carriers
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("GBA CARRIERS", style=_section_label_style),
                    dbc.RadioItems(
                        id="gba_filter_mode",
                        options=[
                            {"label": "Included", "value": "included"},
                            {"label": "Excluded", "value": "excluded"},
                        ],
                        value="included",
                        inline=False,
                    ),
                ],
            ),
            _section_divider,
            # Data transformation
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("DATA TRANSFORMATION", style=_section_label_style),
                    dbc.RadioItems(
                        id="transform",
                        options=[
                            {"label": "None", "value": "none"},
                            {"label": "log", "value": "log"},
                        ],
                        value=DEFAULT_TRANSFORM_VALUE,
                        inline=True,
                    ),
                ],
            ),
            _section_divider,
            # Outlier removal
            html.Div(
                style={"marginBottom": "10px"},
                children=[
                    html.Div("OUTLIER REMOVAL", style=_section_label_style),
                    dbc.RadioItems(
                        id="outlier_removal",
                        options=[
                            {"label": "None", "value": "none"},
                            {"label": "± 3 SD", "value": "std"},
                            {"label": "1.5 IQR", "value": "iqr"},
                        ],
                        value=DEFAULT_OUTLIER_VALUE,
                        inline=True,
                    ),
                ],
            ),
        ],
    )


def _empty_figure(title: str):
    fig = px.scatter(title=title)
    fig.update_layout(template="plotly_white")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


@callback(
    Output("testname", "value"),
    Input("analysis-url", "search"),
    prevent_initial_call=False,
)
def set_testname_from_url(search: str | None):
    if not search:
        return DEFAULT_TESTNAME
    params = urllib.parse.parse_qs(search.lstrip("?"))
    testname = params.get("testname", [None])[0]
    if not testname:
        return DEFAULT_TESTNAME
    testname = urllib.parse.unquote(testname)
    projectid = params.get("projectid", [None])[0]
    if projectid:
        projectid = urllib.parse.unquote(projectid)
    composite = f"{testname}||{projectid}" if projectid else testname
    if composite in TESTNAMES:
        return composite
    if testname in TESTNAMES:
        return testname
    return DEFAULT_TESTNAME


@callback(Output("transform", "value"), Input("testname", "value"))
def set_transform_from_regression_config(testname: str | None):
    return _transform_radio_value_for_testname(testname)


@callback(Output("outlier_removal", "value"), Input("testname", "value"))
def set_outlier_removal_from_regression_config(testname: str | None):
    return _outlier_radio_value_for_testname(testname)


@callback(Output("cohort_filter", "value"), Input("groupby", "value"))
def set_default_cohort_filter(groupby: str | None):
    if groupby == "HEURISTIC":
        return ["PD"] if "PD" in COHORT_VALUES else COHORT_VALUES
    return COHORT_VALUES


@callback(Output("gba_filter_mode", "value"), Input("groupby", "value"))
def set_default_gba_filter_mode(groupby: str | None):
    if groupby == "HEURISTIC":
        return "excluded"
    return "included"


def _filtered_df(
    testname: str,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    project_id: str | None = None,
) -> pd.DataFrame:
    with _timed(
        "analysis.fetch_subset",
        testname=str(testname),
        n_cohorts=len(cohort_filter or []),
        gba_filter_mode=gba_filter_mode,
    ):
        dff = fetch_analysis_subset(
            _engine(),
            testname=str(testname),
            cohort_filter=cohort_filter,
            gba_filter_mode=gba_filter_mode,
            project_id=project_id,
        )
    if dff.empty:
        return dff
    with _timed("analysis.normalize_df", nrows=int(len(dff))):
        return _normalize_analysis_df(dff)


def _apply_transform_and_outliers(
    dff: pd.DataFrame,
    transform: str | None,
    outlier_handling: str | None,
) -> tuple[pd.DataFrame, str]:
    if dff.empty:
        return dff, "TESTVALUE"
    method = outlier_handling if outlier_handling in ("none", "std", "iqr") else "none"
    model_df = dff.copy()
    if transform == "log":
        model_df = model_df[model_df["TESTVALUE"] > 0].copy()
        if model_df.empty:
            return model_df, "LOG_TESTVALUE"
        model_df["LOG_TESTVALUE"] = np.log(model_df["TESTVALUE"])
        testvalue_col = "LOG_TESTVALUE"
    else:
        testvalue_col = "TESTVALUE"
    model_df = drop_outlier_rows(
        model_df, method=method, value_col=testvalue_col, group_col="TESTNAME"
    )
    return model_df, testvalue_col


def _build_model_df(
    testname: str,
    groupby: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    transform: str | None,
    outlier_handling: str | None,
) -> tuple[pd.DataFrame, str, str]:
    group_col = groupby if groupby in {"COHORT", "HEURISTIC"} else "COHORT"
    base = _filtered_df(testname, cohort_filter, gba_filter_mode).copy()
    model_df, testvalue_col = _apply_transform_and_outliers(base, transform, outlier_handling)
    return model_df, group_col, testvalue_col


def _group_config(group_col: str, dff: pd.DataFrame, selected_cohorts: list[str]):
    group_col = group_col if group_col in COHORTS else "COHORT"

    base_order = COHORTS[group_col]["Order"]
    if group_col == "COHORT":
        ordered_values = [c for c in base_order if c in (selected_cohorts or [])]
    else:
        present = set(dff[group_col].astype(str).unique().tolist())
        ordered_values = [v for v in base_order if v in present]
        ordered_values += sorted([v for v in present if v not in set(base_order)])

    category_orders = {group_col: ordered_values}

    base_colors: dict[str, str] = COHORTS[group_col]["Colors"]
    if group_col == "COHORT":
        color_map = {k: v for k, v in base_colors.items() if k in (selected_cohorts or [])}
    else:
        color_map = base_colors

    return category_orders, color_map


@callback(
    [Output("biomarker-title", "children"), Output("biomarker-meta", "children")],
    Input("testname", "value"),
)
def update_biomarker_header(testname: str | None):
    parsed_testname, parsed_project_id = _parse_testname_select_value(testname)
    if not parsed_testname:
        return "", ""

    project_id = (
        parsed_project_id
        if parsed_project_id is not None
        else _modal_project_id_for_testname(str(parsed_testname))
    )

    meta_children: list = []
    if project_id is None:
        meta_children = [html.Div("Project ID: (unknown)")]
        return str(parsed_testname), meta_children

    meta_children.append(html.Div(f"Project ID: {project_id}"))
    info = _cached_projects_lookup().get(project_id)
    if info:
        meta_children.append(html.Div(f"PI Name: {info.get('PI_NAME', '')}"))
        meta_children.append(html.Div(f"PI Institution: {info.get('PI_INSTITUTION', '')}"))
    else:
        meta_children.append(html.Div("PI Name: (unknown)"))
        meta_children.append(html.Div("PI Institution: (unknown)"))

    # Temporarily disabled: project run-date lookup can be slow on remote MySQL.
    # date_info = _cached_project_rundates(project_id)
    # if date_info and date_info.get("min_date") is not None and date_info.get("max_date") is not None:
    #     min_d = pd.to_datetime(date_info["min_date"]).date().isoformat()
    #     max_d = pd.to_datetime(date_info["max_date"]).date().isoformat()
    #     meta_children.append(html.Div(f"Run dates: {min_d} to {max_d}"))
    # else:
    #     meta_children.append(html.Div("Run dates: (unknown)"))

    return str(parsed_testname), meta_children


@memoize(timeout=6 * 60 * 60)
def _cached_projects_lookup() -> dict[str, dict[str, str]]:
    with _timed("header.projects_lookup.query"):
        return get_projects_lookup(_engine())


@memoize(timeout=6 * 60 * 60)
def _cached_project_rundates(project_id: str) -> dict[str, Any] | None:
    with _timed("header.project_rundates.query", project_id=str(project_id)):
        return get_project_rundates_for_project(_engine(), project_id=str(project_id))


layout = html.Div(
    style={"minHeight": "75vh", "display": "flex", "flexDirection": "column"},
    children=[
        dcc.Location(id="analysis-url", refresh=False),
        dbc.Row(
            style={"flex": "1 1 auto", "alignItems": "stretch"},
            children=[
                dbc.Col(
                    children=[
                        description_card(),
                        html.Div(style={"height": "12px"}),
                        generate_control_card(),
                    ],
                    width=3,
                    style={
                        "backgroundColor": "#f8f9fa",
                        "borderRight": "1px solid #e9ecef",
                        "padding": "16px 12px",
                        "height": "100%",
                    },
                ),
                dbc.Col(
                    [
                        dcc.Store(id="analysis-store"),
                        html.Div(
                            [
                                html.H3(
                                    id="biomarker-title",
                                    style={
                                        "margin": "0",
                                        "fontSize": "22px",
                                        "fontWeight": "600",
                                        "color": "#1a1a1a",
                                    },
                                ),
                                html.Div(
                                    id="biomarker-meta",
                                    style={
                                        "marginTop": "6px",
                                        "fontSize": "12px",
                                        "color": "#6c757d",
                                        "display": "flex",
                                        "gap": "16px",
                                    },
                                ),
                            ],
                            style={"marginBottom": "10px"},
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="hist", style={"height": "520px"}), width=6),
                                dbc.Col(dcc.Graph(id="box", style={"height": "520px"}), width=6),
                            ],
                            align="start",
                        ),
                        html.Div(
                            id="stats-results",
                            style={
                                "marginTop": "14px",
                                "backgroundColor": "#fafafa",
                                "borderRadius": "6px",
                                "border": "1px solid #f0f0f0",
                                "padding": "16px",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(style={"height": "24px"}),
                                dbc.Button(
                                    "Download data as a CSV",
                                    id="download-data-btn",
                                    color="secondary",
                                    size="sm",
                                    className="mt-2",
                                ),
                                dcc.Download(id="download-data"),
                            ]
                        ),
                        html.Div(style={"height": "24px"}),
                    ]
                ),
            ],
        ),
    ],
)


@callback(
    Output("analysis-store", "data"),
    [
        Input("testname", "value"),
        Input("cohort_filter", "value"),
        Input("gba_filter_mode", "value"),
    ],
)
def load_analysis_subset_store(
    testname: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
):
    parsed_testname, parsed_project_id = _parse_testname_select_value(testname)
    if not parsed_testname:
        return None
    dff = _filtered_df(str(parsed_testname), cohort_filter, gba_filter_mode, project_id=parsed_project_id)
    if dff.empty:
        return None
    with _timed("analysis.store.serialize", nrows=int(len(dff))):
        return dff.to_json(date_format="iso", orient="split")


@callback(
    [Output("hist", "figure"), Output("box", "figure")],
    [
        Input("analysis-store", "data"),
        Input("groupby", "value"),
        Input("cohort_filter", "value"),
        Input("transform", "value"),
        Input("outlier_removal", "value"),
    ],
)
def update_figures(
    store_json: str | None,
    groupby: str | None,
    cohort_filter: list[str] | None,
    transform: str | None,
    outlier_handling: str | None,
):
    if not store_json:
        empty = _empty_figure("No data to display")
        return empty, empty

    selected_cohorts = cohort_filter or []
    with _timed("analysis.store.deserialize.figures"):
        dff = pd.read_json(StringIO(store_json), orient="split")
    if dff.empty:
        empty = _empty_figure("No rows match current filters")
        return empty, empty

    testname = str(dff["TESTNAME"].iloc[0]) if "TESTNAME" in dff.columns and not dff.empty else ""
    unit = dff["UNITS"].unique()[0]
    with _timed("figures.apply_transform_outliers", nrows=int(len(dff))):
        dff, x_col = _apply_transform_and_outliers(dff, transform, outlier_handling)
    if dff.empty:
        empty = _empty_figure("No rows match current filters")
        return empty, empty
    x_label = f"log({testname}) [{unit}]" if x_col == "LOG_TESTVALUE" else f"{testname} [{unit}]"

    group_col = groupby if groupby in {"COHORT", "HEURISTIC"} else "COHORT"
    category_orders, color_map = _group_config(group_col, dff, selected_cohorts)

    group_order = category_orders.get(group_col, sorted(dff[group_col].unique().tolist()))
    hist_data = [dff.loc[dff[group_col] == g, x_col].astype(float).tolist() for g in group_order]
    hist_labels = [str(g) for g in group_order]

    if color_map:
        colors = [color_map.get(lbl) for lbl in hist_labels]
    else:
        palette = px.colors.qualitative.Plotly
        colors = [palette[i % len(palette)] for i in range(len(hist_labels))]

    with _timed("figures.distplot.build", nrows=int(len(dff))):
        # Filter out groups with constant values (zero variance) — KDE fails on these
        kde_mask = [len(set(d)) > 1 for d in hist_data]
        kde_hist_data = [d for d, ok in zip(hist_data, kde_mask) if ok]
        kde_labels = [lbl for lbl, ok in zip(hist_labels, kde_mask) if ok]
        kde_colors = [c for c, ok in zip(colors, kde_mask) if ok]
        try:
            dist_fig = ff.create_distplot(
                hist_data=kde_hist_data,
                group_labels=kde_labels,
                colors=kde_colors,
                show_hist=True,
                show_rug=False,
                bin_size=freedman_bin_width(dff[x_col]),
            )
            # Add back constant-value groups as histogram-only traces
            for d, lbl, clr in zip(hist_data, hist_labels, colors):
                if len(set(d)) <= 1:
                    dist_fig.add_bar(x=d, name=lbl, marker_color=clr, showlegend=True)
        except Exception:
            dist_fig = ff.create_distplot(
                hist_data=hist_data,
                group_labels=hist_labels,
                colors=colors,
                show_hist=True,
                show_rug=False,
                show_curve=False,
                bin_size=freedman_bin_width(dff[x_col]),
            )
    dist_fig.update_layout(
        template="plotly_white",
        title="Histogram",
        xaxis_title=x_label,
        legend_title_text=group_col,
        margin=dict(l=40, r=20, t=60, b=40),
    )

    with _timed("figures.box.build", nrows=int(len(dff))):
        box_fig = px.box(
            dff,
            x=group_col,
            y=x_col,
            points="all",
            color=group_col,
            category_orders=category_orders,
            color_discrete_map=color_map,
            labels={group_col: group_col, x_col: x_label},
            title="Boxplots",
        )
    box_fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
        showlegend=False,
    )
    box_fig.update_traces(hovertemplate=f"{group_col}=%{{x}}<br>Value=%{{y}}<extra></extra>")
    return dist_fig, box_fig


@callback(
    Output("stats-results", "children"),
    [
        Input("analysis-store", "data"),
        Input("groupby", "value"),
        Input("cohort_filter", "value"),
        Input("transform", "value"),
        Input("outlier_removal", "value"),
    ],
)
def update_stats_table(
    store_json: str | None,
    groupby: str | None,
    cohort_filter: list[str] | None,
    transform: str | None,
    outlier_handling: str | None,
):
    if not store_json:
        return html.Div("Select a biomarker to see cohort regression results.")

    with _timed("analysis.store.deserialize.stats"):
        base = pd.read_json(StringIO(store_json), orient="split")
    if base.empty:
        return html.Div("No rows match current filters; regression tests are unavailable.")

    group_col = groupby if groupby in {"COHORT", "HEURISTIC"} else "COHORT"
    with _timed("stats.apply_transform_outliers", nrows=int(len(base))):
        model_df, testvalue_col = _apply_transform_and_outliers(base, transform, outlier_handling)
    if model_df.empty:
        return html.Div("No rows match current filters; regression tests are unavailable.")

    z_col = "LOG_TESTVALUE_Z" if testvalue_col == "LOG_TESTVALUE" else "TESTVALUE_Z"

    if group_col == "COHORT":
        selected = cohort_filter or []
        cohort_categories = [c for c in COHORTS["COHORT"]["Order"] if c in selected]
    else:
        present = set(model_df["HEURISTIC"].astype(str).unique().tolist())
        cohort_categories = [c for c in COHORTS["HEURISTIC"]["Order"] if c in present]
        cohort_categories += sorted(
            [c for c in present if c not in set(COHORTS["HEURISTIC"]["Order"])]
        )

    try:
        with _timed("stats.regression.run", nrows=int(len(model_df)), group_col=group_col):
            omnibus_df, pairwise_df = run_biomarker_by_biomarker_cohort_regressions(
                model_df,
                standardize_within_biomarker=True,
                cohort_col=group_col,
                cohort_categories=cohort_categories,
                testvalue_col=testvalue_col,
                z_col=z_col,
                outlier_handling="none",
            )
    except Exception as e:
        return html.Div(f"Regression failed: {e}")

    if omnibus_df.empty or pairwise_df.empty:
        return html.Div("No regression results available for the current selection.")

    omnibus_n = omnibus_df["n"].iloc[0]
    omnibus_pval = omnibus_df["omnibus_pval"].iloc[0]

    def _format_pval(p: object) -> str | None:
        if p is None:
            return None
        try:
            pv = float(p)
        except Exception:
            return None
        if not np.isfinite(pv):
            return None
        if pv < 1e-4:
            return f"{pv:.2e}"
        return f"{pv:.6f}"

    table_kwgs = {
        "page_size": 10,
        "style_table": {
            "overflowX": "auto",
            "width": "auto",
            "display": "inline-block",
            "maxWidth": "100%",
        },
        "style_cell": {
            "textAlign": "left",
            "paddingLeft": "12px",
            "paddingRight": "12px",
            "whiteSpace": "nowrap",
        },
        "style_header": {"fontWeight": "bold"},
    }

    summary_df = (
        model_df.groupby(group_col, dropna=False)[testvalue_col]
        .agg(n="count", mean="mean", median="median")
        .reset_index()
        .rename(columns={group_col: "group"})
    )

    group_order = cohort_categories
    if group_order:
        summary_df["group"] = pd.Categorical(summary_df["group"], categories=group_order, ordered=True)
        summary_df = summary_df.sort_values("group")

    summary_df["mean"] = pd.to_numeric(summary_df["mean"], errors="coerce").round(4)
    summary_df["median"] = pd.to_numeric(summary_df["median"], errors="coerce").round(4)

    summary_table = dash_table.DataTable(
        columns=[
            {"name": group_col, "id": "group"},
            {"name": "n", "id": "n"},
            {"name": "mean", "id": "mean"},
            {"name": "median", "id": "median"},
        ],
        data=summary_df.where(pd.notna(summary_df), None).to_dict("records"),
        **table_kwgs,
    )

    table_rows: list[dict[str, object]] = [
        {
            "type": "omnibus",
            "comparison": "All cohorts (F-test)" if group_col == "COHORT" else "All groups (F-test)",
            "n": int(omnibus_n),
            "beta": None,
            "pval": _format_pval(omnibus_pval),
            "effect_size_std": None,
        }
    ]

    for _, r in pairwise_df.iterrows():
        table_rows.append(
            {
                "type": "pairwise",
                "comparison": str(r["comparison"]),
                "n": float(r["n"]) if pd.notna(r["n"]) else None,
                "beta": float(r["beta"]) if pd.notna(r["beta"]) else None,
                "pval": _format_pval(r["pval"]) if pd.notna(r["pval"]) else None,
                "effect_size_std": float(r["effect_size_std"]) if pd.notna(r["effect_size_std"]) else None,
            }
        )

    table_df = pd.DataFrame(table_rows)
    for col in ["beta", "effect_size_std"]:
        if col in table_df.columns:
            table_df[col] = pd.to_numeric(table_df[col], errors="coerce").round(6)

    stats_table = dash_table.DataTable(
        columns=[
            {"name": "Type", "id": "type"},
            {"name": "Comparison", "id": "comparison"},
            {"name": "n", "id": "n"},
            {"name": "beta", "id": "beta"},
            {"name": "pval", "id": "pval"},
            {"name": "effect_size_std", "id": "effect_size_std"},
        ],
        data=table_df.where(pd.notna(table_df), None).to_dict("records"),
        **table_kwgs,
    )

    return html.Div(
        [
            html.H6("Summary table", style={"marginTop": "0px"}),
            summary_table,
            html.Div(style={"height": "24px"}),
            html.H6("Statistical tests", style={"marginTop": "0px"}),
            stats_table,
        ]
    )


@callback(
    Output("download-data", "data"),
    Input("download-data-btn", "n_clicks"),
    [
        State("testname", "value"),
        State("groupby", "value"),
        State("cohort_filter", "value"),
        State("gba_filter_mode", "value"),
        State("transform", "value"),
        State("outlier_removal", "value"),
    ],
    prevent_initial_call=True,
)
def download_filtered_data(
    _n_clicks: int | None,
    testname: str | None,
    groupby: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    transform: str | None,
    outlier_handling: str | None,
):
    parsed_testname, parsed_project_id = _parse_testname_select_value(testname)
    if not parsed_testname:
        return None

    model_df, group_col, testvalue_col = _build_model_df(
        str(parsed_testname),
        groupby,
        cohort_filter,
        gba_filter_mode,
        transform,
        outlier_handling,
    )
    if model_df.empty:
        return None

    cols = [
        "PATNO",
        "CLINICAL_EVENT",
        "AGE_AT_VISIT",
        "SEX",
        "COHORT",
        "HEURISTIC",
        "GBA",
        "TESTNAME",
        "TESTVALUE",
        "UNITS",
        "RUNDATE",
        "PROJECTID",
    ]
    if "LOG_TESTVALUE" in model_df.columns:
        cols.insert(cols.index("TESTVALUE") + 1, "LOG_TESTVALUE")

    export_df = model_df.loc[:, [c for c in cols if c in model_df.columns]].copy()
    export_df["group_col"] = group_col
    export_df["testvalue_col"] = testvalue_col

    if "RUNDATE" in export_df.columns:
        export_df["RUNDATE"] = pd.to_datetime(export_df["RUNDATE"], errors="coerce").dt.date.astype(
            "string"
        )

    safe_name = str(parsed_testname).replace("/", "-").replace("\\", "-")
    if parsed_project_id is not None:
        safe_name = f"{safe_name}_project-{int(parsed_project_id)}"
    filename = (
        f"{safe_name}__groupby-{group_col}__transform-{transform or 'none'}"
        f"__outliers-{outlier_handling or 'none'}.csv"
    )
    return dcc.send_data_frame(export_df.to_csv, filename, index=False)

