from __future__ import annotations

import dash
import dash_bootstrap_components as dbc
from dash import html


dash.register_page(__name__, path="/", name="Home", title="Home | Biomarker Dashboard")


def _term(title: str, body: str):
    return html.Li([html.Strong(f"{title}: "), html.Span(body)], style={"marginBottom": "8px"})


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
                                html.H4("How to use"),
                                html.Ul(
                                    [
                                        _term(
                                            "Select Biomarker",
                                            "Choose a biomarker from the dropdown on the Biomarker Analysis page.",
                                        ),
                                        _term(
                                            "Compare groups by",
                                            "Choose whether to group by PD diagnosis (COHORT) or classifier result (HEURISTIC).",
                                        ),
                                        _term(
                                            "Cohorts to include",
                                            "Limit the analysis to one or more cohorts.",
                                        ),
                                        _term(
                                            "GBA carriers",
                                            "Include or exclude participants with GBA carrier status.",
                                        ),
                                        _term(
                                            "Data transformation",
                                            "Optionally apply a log transform; this requires positive values.",
                                        ),
                                        _term(
                                            "Download data",
                                            "Export the filtered rows used for plots and statistics as a CSV.",
                                        ),
                                    ]
                                ),
                            ]
                        )
                    ),
                    lg=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H4("Statistical terms used here"),
                                html.Ul(
                                    [
                                        _term("n", "Number of observations used in the analysis."),
                                        _term(
                                            "Omnibus test",
                                            "A single test across all groups (e.g., an F-test) that asks whether any group differs.",
                                        ),
                                        _term(
                                            "Pairwise comparison",
                                            "A comparison between two specific groups (e.g., Group A vs Group B).",
                                        ),
                                        _term(
                                            "beta",
                                            "An estimated effect size from the regression model (direction and magnitude).",
                                        ),
                                        _term(
                                            "p-value",
                                            "Evidence against the null hypothesis for a single test; smaller values indicate stronger evidence.",
                                        ),
                                        _term(
                                            "q-value (FDR BH)",
                                            "A multiple-testing adjusted p-value using Benjamini–Hochberg false discovery rate control.",
                                        ),
                                        _term(
                                            "Standardized effect size",
                                            "Effect size on a standardized scale to make magnitudes more comparable across biomarkers.",
                                        ),
                                    ]
                                ),
                            ]
                        )
                    ),
                    lg=6,
                ),
            ],
            style={"marginBottom": "18px"},
        ),
        dbc.Row(
            [
                dbc.Col(
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
                    ),
                    lg=12,
                )
            ]
        ),
    ],
    style={"paddingBottom": "24px"},
)

