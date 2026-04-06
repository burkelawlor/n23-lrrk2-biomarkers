from __future__ import annotations

from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import dash_ag_grid as dag
from dash import html


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

    merge_keys = ["PROJECTID", "TESTNAME", "gba_included", "log_transform", "pd_only"]
    pairwise_non_vs_pred = pairwise_non_vs_pred[
        merge_keys + ["pval", "qval_fdr_bh", "n"]
    ].rename(
        columns={
            "pval": "non_vs_predicted_pval",
            "qval_fdr_bh": "non_vs_predicted_qval_fdr_bh",
            "n": "n_non_vs_predicted",
        }
    )

    merged = omnibus_df.merge(pairwise_non_vs_pred, on=merge_keys, how="left")
    return merged


def _build_table(df: pd.DataFrame):
    display_df = df.copy()

    sciFormatter = {"function": "d3.format('.4')(params.value)"}

    int_kwgs = {"type": "numericColumn", "filter": "agNumberColumnFilter", "align": "left"}
    float_kwgs = {"type": "numericColumn", "filter": "agNumberColumnFilter", "valueFormatter": sciFormatter}

    column_defs = [
        {"field": "PROJECTID", "headerName": "PROJECTID", "flex": 1, "minWidth": 110},
        {"field": "TESTNAME", "headerName": "TESTNAME", "flex": 3, "minWidth": 260},
        {"field": "n", "headerName": "n_omnibus", "flex": 1, "minWidth": 130, **int_kwgs},
        {"field": "omnibus_pval", "headerName": "pval_omnibus", "flex": 1, "minWidth": 160, **float_kwgs},
        {"field": "omnibus_qval_fdr_bh", "headerName": "qval_omnibus", "flex": 1, "minWidth": 170, **float_kwgs},
        {"field": "n_non_vs_predicted", "headerName": "n_non_vs_predicted", "flex": 1, "minWidth": 190, **int_kwgs},
        {"field": "non_vs_predicted_pval", "headerName": "pval_non_vs_predicted", "flex": 1, "minWidth": 210, **float_kwgs},
        {
            "field": "non_vs_predicted_qval_fdr_bh",
            "headerName": "qval_non_vs_predicted",
            "flex": 1,
            "minWidth": 220,
            **float_kwgs,
        },
        # {"field": "cohort_col", "headerName": "Cohort"},
        # {"field": "gba_included", "filter": "agSetColumnFilter"},
        # {"field": "log_transform", "filter": "agSetColumnFilter"},
        # {"field": "pd_only", "filter": "agSetColumnFilter"},
    ]

    grid = dag.AgGrid(
        id="omnibus-grid",
        className="ag-theme-alpine",
        columnSize="sizeToFit",
        columnDefs=column_defs,
        rowData=display_df.where(pd.notna(display_df), None).to_dict("records"),
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
            [
                dbc.Col(
                    [
                        html.H2("Results Overview"),
                        html.P(
                            [
                                "Interactive view of regression results loaded from omnibus and pairwise tests ",
                                html.Code("output/regression_results_omnibus_HEURISTIC.csv"),
                                " and ",
                                html.Code("output/regression_results_pairwise_HEURISTIC.csv"),
                                ". Use the column filters and sorting to find biomarkers of interest.",
                            ]
                        ),
                    ],
                    lg=10,
                )
            ],
            style={"marginBottom": "10px"},
        ),
        dbc.Row([dbc.Col(_build_table(_df), lg=12)]),
        html.Div(style={"height": "18px"}),
    ],
)

