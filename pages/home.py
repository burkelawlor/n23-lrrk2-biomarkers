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
    included_ids = set(db_df["PROJECTID"].astype(int))

    xlsx_path = Path(__file__).parent.parent / "PPMI Completed and Ongoing Biologic Analyses 042026.xlsx"
    raw = pd.read_excel(xlsx_path, header=0)
    raw = raw.rename(columns={raw.columns[4]: "Project ID"})

    def matches(val):
        if pd.isna(val):
            return False
        for part in str(val).split(","):
            try:
                if int(part.strip()) in included_ids:
                    return True
            except ValueError:
                pass
        return False

    filtered = raw[raw["Project ID"].apply(matches)].copy()
    cols = ["Project ID", "Project Title", "Principal Investigator", "Organization", "Analyte", "Matrix", "Visit(s)"]
    return filtered[cols].reset_index(drop=True)


_projects_df = _load_projects_table()


layout = dbc.Container(
    fluid=True,
    children=[
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2("Neuron23 LRRK2 Biomarker Dashboard"),
                        html.P(
                            "This dashboard helps you explore biomarker distributions and statistical differences "
                            "between participant groups across projects."
                        ),
                    ],
                    md=10,
                    lg=8,
                )
            ],
            style={"marginBottom": "18px"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("About the dashboard"),
                                html.P(
                                    [
                                        html.Strong("Results Overview: "),
                                        "A pre-computed table of regression results across all projects and biomarkers, "
                                        "sortable and filterable, ranked by omnibus q-value (BH-FDR corrected).",
                                    ],
                                    style={"marginBottom": "8px"},
                                ),
                                html.P(
                                    [
                                        html.Strong("Biomarker Analysis: "),
                                        "An interactive page for exploring individual biomarkers. Select a biomarker, "
                                        "apply filters (cohort, GBA carrier status, log transform, outlier handling), "
                                        "and view distributions alongside live pairwise statistical comparisons.",
                                    ],
                                    style={"marginBottom": "16px"},
                                ),
                                dbc.Alert(
                                    [
                                        html.Strong("Tip: "),
                                        html.Span(
                                            "Start with the Results Overview page to see which biomarkers show the strongest "
                                            "signals (lowest q-values), then drill into Biomarker Analysis for distributions "
                                            "and detailed pairwise tests."
                                        ),
                                    ],
                                    color="info",
                                    style={"marginBottom": 0},
                                ),
                            ]
                        )
                    ),
                    lg=8,
                )
            ],
            style={"marginBottom": "24px"},
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H4("Included Projects"),
                        html.P(
                            "PPMI biologic analyses included in this dashboard, sourced from the PPMI Completed and Ongoing Biologic Analyses registry.",
                            className="text-muted",
                            style={"marginBottom": "10px"},
                        ),
                        html.Div(
                            dbc.Table.from_dataframe(
                                _projects_df,
                                striped=True,
                                bordered=False,
                                hover=True,
                                size="sm",
                            ),
                            style={"overflowX": "auto"},
                        ),
                    ],
                    lg=12,
                )
            ]
        ),
    ],
    style={"paddingBottom": "24px"},
)
