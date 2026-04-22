from __future__ import annotations

from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import html

from utils.db_runtime import get_engine_from_env, get_projects_df


dash.register_page(__name__, path="/", name="Home", title="Home | Biomarker Dashboard")


def _load_projects_table() -> pd.DataFrame:
    engine = get_engine_from_env()
    db_df = get_projects_df(engine)
    # Build a set of the bare numeric portions of PROJECTID (e.g. "145" from "PPMI 145")
    # so we can match against the Excel registry which uses plain integers.
    included_numeric_ids: set[int] = set()
    for pid in db_df["PROJECTID"].dropna():
        parts = str(pid).split()
        for part in parts:
            try:
                included_numeric_ids.add(int(part))
            except ValueError:
                pass

    xlsx_path = Path(__file__).parent.parent / "PPMI Completed and Ongoing Biologic Analyses 042026.xlsx"
    raw = pd.read_excel(xlsx_path, header=0)
    raw = raw.rename(columns={raw.columns[4]: "Project ID"})

    def matches(val):
        if pd.isna(val):
            return False
        for part in str(val).split(","):
            try:
                if int(part.strip()) in included_numeric_ids:
                    return True
            except ValueError:
                pass
        return False

    filtered = raw[raw["Project ID"].apply(matches)].copy()
    cols = ["Project ID", "Project Title", "Principal Investigator", "Organization", "Analyte", "Matrix", "Visit(s)"]
    return filtered[cols].reset_index(drop=True)


_projects_df = _load_projects_table()


# ── shared style tokens ──────────────────────────────────────────────────────
_ACCENT = "#3a6ea5"          # nav-compatible steel blue
_HERO_BG = "#eef2f7"         # soft blue-gray hero background
_DIVIDER = {"borderTop": "1px solid #dee2e6", "margin": "0"}

# ── feature-card helper ───────────────────────────────────────────────────────
def _feature_item(icon: str, title: str, body: str) -> html.Div:
    return html.Div(
        [
            html.Div(
                [
                    html.Span(icon, style={"fontSize": "1.25rem", "lineHeight": 1}),
                    html.Strong(title, style={"marginLeft": "8px", "fontSize": "0.95rem"}),
                ],
                style={"display": "flex", "alignItems": "center", "marginBottom": "4px"},
            ),
            html.P(
                body,
                style={
                    "margin": "0",
                    "fontSize": "0.875rem",
                    "color": "#495057",
                    "lineHeight": "1.55",
                },
            ),
        ],
        style={
            "borderLeft": f"3px solid {_ACCENT}",
            "paddingLeft": "12px",
            "marginBottom": "16px",
        },
    )


layout = dbc.Container(
    fluid=True,
    style={"paddingBottom": "40px", "paddingLeft": 0, "paddingRight": 0},
    children=[

        # ── Hero banner ───────────────────────────────────────────────────────
        html.Div(
            dbc.Container(
                fluid="xl",
                children=dbc.Row(
                    dbc.Col(
                        [
                            html.H2(
                                "LRRK2 Biomarker Dashboard",
                                style={
                                    "fontWeight": 700,
                                    "color": "#1a2b3c",
                                    "marginBottom": "8px",
                                    "fontSize": "1.6rem",
                                },
                            ),
                            html.P(
                                "Explore distributions and statistical differences "
                                "between PD and LRRK2 groups across biomarkers.",
                                style={
                                    "color": "#4a5568",
                                    "fontSize": "1rem",
                                    "marginBottom": 0,
                                },
                            ),
                        ],
                    ),
                    align="center",
                ),
            ),
            style={
                "backgroundColor": _HERO_BG,
                "padding": "32px 24px 28px",
                "borderBottom": f"2px solid #d1dce8",
                "marginBottom": "32px",
            },
        ),

        # ── Body content (constrained width) ─────────────────────────────────
        dbc.Container(
            fluid="xl",
            style={"paddingLeft": "24px", "paddingRight": "24px"},
            children=[

                # About card
                dbc.Row(
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Span(
                                        "About the Dashboard",
                                        style={"fontWeight": 600, "fontSize": "1rem", "color": "#1a2b3c"},
                                    ),
                                    style={
                                        "backgroundColor": "#f8fafc",
                                        "borderBottom": "1px solid #e2e8f0",
                                        "padding": "12px 20px",
                                    },
                                ),
                                dbc.CardBody(
                                    [
                                        _feature_item(
                                            "\U0001f4cb",
                                            "Results Overview",
                                            "A pre-computed table of regression results across all projects and biomarkers, "
                                            "sortable and filterable, ranked by omnibus q-value (BH-FDR corrected).",
                                        ),
                                        _feature_item(
                                            "\U0001f9ea",
                                            "Biomarker Analysis",
                                            "An interactive page for exploring individual biomarkers. Select a biomarker, "
                                            "apply filters (cohort, GBA carrier status, log transform, outlier handling), "
                                            "and view distributions alongside live pairwise statistical comparisons.",
                                        ),
                                        # Tip alert
                                        html.Div(
                                            [
                                                html.Span(
                                                    "\U0001f4a1",
                                                    style={"marginRight": "6px", "fontSize": "0.85rem"},
                                                ),
                                                html.Strong(
                                                    "Tip: ",
                                                    style={"fontSize": "0.8rem"},
                                                ),
                                                html.Span(
                                                    "Start with Results Overview to find biomarkers with the strongest signals "
                                                    "(lowest q-values), then drill into Biomarker Analysis for distributions "
                                                    "and pairwise tests.",
                                                    style={"fontSize": "0.8rem"},
                                                ),
                                            ],
                                            style={
                                                "backgroundColor": "#eff6ff",
                                                "border": "1px solid #bfdbfe",
                                                "borderRadius": "6px",
                                                "padding": "10px 14px",
                                                "color": "#1e40af",
                                                "lineHeight": "1.5",
                                            },
                                        ),
                                    ],
                                    style={"padding": "20px"},
                                ),
                            ],
                            style={
                                "border": "1px solid #e2e8f0",
                                "borderRadius": "8px",
                                "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
                            },
                        ),
                        lg=12,
                    ),
                    style={"marginBottom": "32px"},
                ),

                # Thin divider
                html.Hr(style=_DIVIDER),
                html.Div(style={"marginBottom": "28px"}),

                # Included projects
                dbc.Row(
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.H5(
                                        "Included Projects",
                                        style={
                                            "fontWeight": 700,
                                            "color": "#1a2b3c",
                                            "marginBottom": "4px",
                                        },
                                    ),
                                    html.P(
                                        "PPMI biologic analyses included in this dashboard, sourced from the "
                                        "PPMI Completed and Ongoing Biologic Analyses registry.",
                                        style={
                                            "color": "#6c757d",
                                            "fontSize": "0.875rem",
                                            "marginBottom": "16px",
                                        },
                                    ),
                                ]
                            ),
                            html.Div(
                                dbc.Table.from_dataframe(
                                    _projects_df,
                                    striped=False,
                                    bordered=False,
                                    hover=True,
                                    size="sm",
                                    class_name="align-middle",
                                ),
                                style={
                                    "overflowX": "auto",
                                    "border": "1px solid #e2e8f0",
                                    "borderRadius": "8px",
                                    "fontSize": "0.82rem",
                                },
                            ),
                        ],
                        lg=12,
                    ),
                ),

            ],
        ),
    ],
)
