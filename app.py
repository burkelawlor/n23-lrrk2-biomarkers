from __future__ import annotations

import os
from dotenv import load_dotenv
import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

try:
    load_dotenv(".env")
except Exception:
    pass


def _header(app: Dash):
    link_style = {
        "color": "#1a1a1a",
        "textDecoration": "none",
        "fontWeight": 600,
        "marginLeft": "14px",
    }
    return dbc.Row(
        children=[
            dbc.Col(
                html.Img(
                    src=app.get_asset_url("Neuron23_Logo.png"),
                    style={"maxWidth": "130px", "height": "auto", "margin": "8px 8px"},
                ),
                width="auto",
            ),
            dbc.Col(
                html.Div(
                    [
                        dcc.Link("Home", href="/", style=link_style),
                        dcc.Link("Results Overview", href="/results", style=link_style),
                        dcc.Link("Biomarker Analysis", href="/analysis", style=link_style),
                    ],
                    style={"display": "flex", "alignItems": "center", "height": "100%"},
                )
            ),
        ],
        align="center",
        style={"backgroundColor": "#f2f2f2", "padding": "6px 0"},
    )


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], use_pages=True)
app.title = "Biomarker Dashboard"

app.layout = html.Div(
    style={"minHeight": "100vh", "display": "flex", "flexDirection": "column"},
    children=[
        dbc.Container(
            fluid=True,
            style={"flex": "1 1 auto", "display": "flex", "flexDirection": "column"},
            children=[
                _header(app),
                html.Div(
                    dash.page_container,
                    style={"flex": "1 1 auto", "paddingTop": "12px"},
                ),
            ],
        )
    ],
)


if __name__ == "__main__":
    # Dash's debug reloader spawns a child process; ensure env is visible there too.
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    app.run(debug=True)

