from __future__ import annotations

from pathlib import Path
import os
import urllib.parse

import dash
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from dash import html, Input, Output, callback

import numpy as np
import pandas as pd

dash.register_page(
    __name__,
    path="/results",
    name="Results Overview",
    title="Results Overview | Biomarker Dashboard",
)


_OMNIBUS_PATH = Path(__file__).resolve().parents[1] / "output" / "regression_results_omnibus_HEURISTIC.csv"
_PAIRWISE_PATH = Path(__file__).resolve().parents[1] / "output" / "regression_results_pairwise_HEURISTIC.csv"


def _format_sci(x: object) -> str | None:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if pd.isna(v):
        return None
    if v == 0:
        return "0"
    if abs(v) < 1e-4:
        return f"{v:.2e}"
    return f"{v:.6f}"


def _load_df() -> pd.DataFrame:
    if not _OMNIBUS_PATH.exists():
        return pd.DataFrame(
            columns=[
                "PROJECTID",
                "cohort_col",
                "TESTNAME",
                "n",
                "omnibus_pval",
                "omnibus_qval_fdr_bh",
                "non_vs_predicted_pval",
                "non_vs_predicted_qval_fdr_bh",
                "n_non_vs_predicted",
                "beta_std_non_vs_predicted",
                "n_non_vs_rv",
                "beta_std_non_vs_rv",
                "pval_non_vs_rv",
                "qval_non_vs_rv",
                "rv_predicted_concurrent",
                "gba_included",
                "log_transform",
                "pd_only",
            ]
        )

    omnibus_df = pd.read_csv(_OMNIBUS_PATH)

    if not _PAIRWISE_PATH.exists():
        return omnibus_df

    pairwise_df = pd.read_csv(_PAIRWISE_PATH)
    pairwise_non_vs_pred = pairwise_df.loc[pairwise_df["comparison"] == "Non vs Predicted"].copy()
    pairwise_non_vs_rv = pairwise_df.loc[pairwise_df["comparison"] == "Non vs RV"].copy()

    merge_keys = ["PROJECTID", "TESTNAME", "gba_included", "log_transform", "pd_only"]
    pairwise_non_vs_pred = pairwise_non_vs_pred[
        merge_keys + ["pval", "qval_fdr_bh", "n", "effect_size_std"]
    ].rename(
        columns={
            "pval": "non_vs_predicted_pval",
            "qval_fdr_bh": "non_vs_predicted_qval_fdr_bh",
            "n": "n_non_vs_predicted",
            "effect_size_std": "beta_std_non_vs_predicted",
        }
    )

    pairwise_non_vs_rv = pairwise_non_vs_rv[
        merge_keys + ["pval", "qval_fdr_bh", "n", "effect_size_std"]
    ].rename(
        columns={
            "n": "n_non_vs_rv",
            "pval": "pval_non_vs_rv",
            "qval_fdr_bh": "qval_non_vs_rv",
            "effect_size_std": "beta_std_non_vs_rv",
        }
    )

    merged = omnibus_df.merge(pairwise_non_vs_pred, on=merge_keys, how="left").merge(
        pairwise_non_vs_rv, on=merge_keys, how="left"
    )

    has_both_betas = merged["beta_std_non_vs_predicted"].notna() & merged["beta_std_non_vs_rv"].notna()
    merged["rv_predicted_concurrent"] = None
    merged.loc[has_both_betas, "rv_predicted_concurrent"] = (
        np.sign(merged.loc[has_both_betas, "beta_std_non_vs_predicted"])
        == np.sign(merged.loc[has_both_betas, "beta_std_non_vs_rv"])
    ).map({True: "Yes", False: "No"})
    return merged


def _make_analysis_url(row) -> str:
    testname = urllib.parse.quote(str(row["TESTNAME"]), safe="")
    projectid = urllib.parse.quote(str(row["PROJECTID"]), safe="")
    return f"/analysis?testname={testname}&projectid={projectid}"


def _build_table(df: pd.DataFrame):
    display_df = df.copy()
    display_df["_analysis_url"] = display_df.apply(_make_analysis_url, axis=1)

    sciFormatter = {"function": "d3.format('.4')(params.value)"}
    betaFormatter = {"function": "d3.format('.3f')(params.value)"}

    int_kwgs = {"type": "numericColumn", "filter": "agNumberColumnFilter", "align": "left"}
    float_kwgs = {"type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": sciFormatter}
    beta_kwgs = {"type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": betaFormatter}

    column_defs = [
        {"field": "PROJECTID", "headerName": "PROJECTID", "flex": 1, "minWidth": 110},
        {"field": "TESTNAME", "headerName": "TESTNAME", "flex": 3, "minWidth": 260, "cellRenderer": "TestNameLink"},
        {"field": "n", "headerName": "n_omnibus", "flex": 1, "minWidth": 130, **int_kwgs},
        {"field": "omnibus_pval", "headerName": "pval_omnibus", "flex": 1, "minWidth": 160, **float_kwgs},
        {"field": "omnibus_qval_fdr_bh", "headerName": "qval_omnibus", "flex": 1, "minWidth": 170, **float_kwgs},
        {"field": "n_non_vs_predicted", "headerName": "n_non_vs_predicted", "flex": 1, "minWidth": 190, **int_kwgs},
        {"field": "beta_std_non_vs_predicted","headerName": "beta_std_non_vs_predicted", "flex": 1, "minWidth": 230, **beta_kwgs,},
        {"field": "non_vs_predicted_pval", "headerName": "pval_non_vs_predicted", "flex": 1, "minWidth": 210, **float_kwgs},
        {"field": "non_vs_predicted_qval_fdr_bh","headerName": "qval_non_vs_predicted", "flex": 1,"minWidth": 220,**float_kwgs,},
        {"field": "n_non_vs_rv", "headerName": "n_non_vs_rv", "flex": 1, "minWidth": 150, **int_kwgs},
        {"field": "beta_std_non_vs_rv", "headerName": "beta_std_non_vs_rv", "flex": 1, "minWidth": 170, **beta_kwgs},
        {"field": "pval_non_vs_rv", "headerName": "pval_non_vs_rv", "flex": 1, "minWidth": 150, **float_kwgs},
        {"field": "qval_non_vs_rv", "headerName": "qval_non_vs_rv", "flex": 1, "minWidth": 150, **float_kwgs},
        {"field": "rv_predicted_concurrent", "headerName": "rv_predicted_concurrent", "flex": 1, "minWidth": 210, "filter": "agSetColumnFilter"},
        # {"field": "cohort_col", "headerName": "Cohort"},
        # {"field": "gba_included", "filter": "agSetColumnFilter"},
        # {"field": "log_transform", "filter": "agSetColumnFilter"},
        # {"field": "pd_only", "filter": "agSetColumnFilter"},
    ]

    grid = dag.AgGrid(
        id="omnibus-grid",
        className="ag-theme-alpine",
        enableEnterpriseModules=True,
        licenseKey=os.getenv("AG_GRID_LICENSE_KEY"),
        columnSize="sizeToFit",
        columnDefs=column_defs,
        rowData=display_df.where(pd.notna(display_df), None).to_dict("records"),
        csvExportParams={"fileName": "results_overview.csv"},
        defaultColDef={
            "sortable": True,
            "filter": True,
            "floatingFilter": True,
            "resizable": True,
            "flex": 1,
            "minWidth": 120,
            "cellStyle": {
                "textAlign": "left",
                "paddingLeft": "10px",
                "paddingRight": "10px",
                "whiteSpace": "nowrap",
                "fontFamily": "system-ui",
                "fontSize": "13px",
            },
        },
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 100,
            "suppressRowClickSelection": True,
            "sideBar": {"toolPanels": ["columns", "filters"], "defaultToolPanel": ""},
            "getRowStyle": {
                "styleConditions": [
                    {
                        "condition": "params.node.rowIndex % 2 === 1",
                        "style": {"backgroundColor": "#fafafa"},
                    }
                ]
            },
        },
        style={"height": "72vh", "width": "100%"},
    )

    return grid


_df = _load_df()
if "omnibus_qval_fdr_bh" in _df.columns:
    _df = _df.sort_values("omnibus_qval_fdr_bh", ascending=True, na_position="last")


layout = dbc.Container(
    fluid=True,
    children=[
        dbc.Row(
            dbc.Col(
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H4(
                                            "Results Overview",
                                            style={"margin": "0", "fontWeight": "600", "color": "#343a40"},
                                        ),
                                        html.P(
                                            "Interactive view of regression results for omnibus and pairwise (Non vs Predicted, Non vs RV) tests. "
                                            "Use the column filters and sorting to find biomarkers of interest.",
                                            style={
                                                "margin": "6px 0 0 0",
                                                "fontSize": "14px",
                                                "color": "#6c757d",
                                            },
                                        ),
                                        dbc.Button(
                                            "Show / hide column descriptions",
                                            id="results-desc-toggle",
                                            color="link",
                                            size="sm",
                                            style={
                                                "padding": "0",
                                                "marginTop": "6px",
                                                "fontSize": "13px",
                                                "textDecoration": "none",
                                                "color": "#0d6efd",
                                            },
                                        ),
                                    ]
                                ),
                                dbc.Button(
                                    "Download CSV",
                                    id="results-download-btn",
                                    color="secondary",
                                    outline=True,
                                    size="sm",
                                    style={"whiteSpace": "nowrap"},
                                ),
                            ],
                            style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start"},
                        ),
                        dbc.Collapse(
                            html.Div(
                                html.Ul(
                                    [
                                        html.Li(
                                            [
                                                "Two tests are performed for each biomarker: ",
                                                html.B("omnibus"),
                                                " and ",
                                                html.B("RV vs Predicted pairwise"),
                                                ". For each test, an n, p-value, and q-value are shown.",
                                            ]
                                        ),
                                        html.Li(
                                            [
                                                "The ",
                                                html.B("omnibus"),
                                                " test checks for a difference across all classifier cohorts (Non, RV, and Predicted). "
                                                "A low p-value indicates a difference in at least one cohort.",
                                            ]
                                        ),
                                        html.Li(
                                            [
                                                "The ",
                                                html.B("Non vs Predicted/RV pairwise"),
                                                " test checks for a difference between the Non and Predicted or Non and RV cohorts.",
                                            ]
                                        ),
                                        html.Li(
                                            [html.B("beta_std"), " columns show effect size estimate in standard deviation units."]
                                        ),
                                        html.Li(
                                            [
                                                html.B("q-values"),
                                                " are FDR-corrected p-values (Benjamini-Hochberg), applied per project to control false positives "
                                                "across thousands of simultaneous comparisons.",
                                            ]
                                        ),
                                        html.Li(
                                            [
                                                html.B("RV vs Predicted Concurrent"),
                                                " indicates whether the Predicted and RV groups trend in the same direction.",
                                            ]
                                        ),
                                    ],
                                    style={"marginBottom": "0", "paddingLeft": "20px", "fontSize": "13px"},
                                ),
                                style={"paddingTop": "10px"},
                            ),
                            id="results-desc-collapse",
                            is_open=False,
                        ),
                    ],
                    style={
                        "backgroundColor": "#f8f9fa",
                        "border": "1px solid #e9ecef",
                        "borderRadius": "6px",
                        "padding": "16px 20px",
                    },
                ),
                lg=12,
            ),
            style={"marginBottom": "14px"},
        ),
        dbc.Row(
            dbc.Col(
                html.Div(
                    _build_table(_df),
                    style={"padding": "0"},
                ),
                lg=12,
            )
        ),
        html.Div(style={"height": "50px"}),
    ],
)


@callback(
    Output("results-desc-collapse", "is_open"),
    Input("results-desc-toggle", "n_clicks"),
    prevent_initial_call=True,
)
def _toggle_desc(n_clicks):
    # Each click flips the state; derive from click parity so it always works
    # even if the callback fires multiple times.
    return (n_clicks or 0) % 2 == 1


@callback(
    Output("omnibus-grid", "exportDataAsCsv"),
    Input("results-download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def _download_csv(n_clicks):
    return True
