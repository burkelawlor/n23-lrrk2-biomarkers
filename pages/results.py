from __future__ import annotations

from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dash_table, html


dash.register_page(
    __name__,
    path="/results",
    name="Results Overview",
    title="Results Overview | Biomarker Dashboard",
)


_OMNIBUS_PATH = Path(__file__).resolve().parents[1] / "output" / "biomarker_cohort_omnibus.csv"


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
                "gba_included",
                "log_transform",
                "pd_only",
            ]
        )
    df = pd.read_csv(_OMNIBUS_PATH)
    return df


def _build_table(df: pd.DataFrame):
    display_df = df.copy()
    for c in ["omnibus_pval", "omnibus_qval_fdr_bh"]:
        if c in display_df.columns:
            display_df[c] = display_df[c].map(_format_sci)

    columns = [{"name": c, "id": c} for c in display_df.columns]

    return dash_table.DataTable(
        id="omnibus-table",
        columns=columns,
        data=display_df.where(pd.notna(display_df), None).to_dict("records"),
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=20,
        fixed_rows={"headers": True},
        style_table={
            "overflowX": "auto",
            "maxWidth": "100%",
            "height": "72vh",
            "overflowY": "auto",
        },
        style_cell={
            "textAlign": "left",
            "paddingLeft": "10px",
            "paddingRight": "10px",
            "whiteSpace": "nowrap",
            "fontFamily": "system-ui",
            "fontSize": "13px",
        },
        style_header={"fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
        ],
    )


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
                                "Interactive view of omnibus regression results loaded from ",
                                html.Code("output/biomarker_cohort_omnibus.csv"),
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

