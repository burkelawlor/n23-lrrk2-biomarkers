from __future__ import annotations

import os
from dotenv import load_dotenv
import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from utils.cache_runtime import init_cache

import dash_mantine_components as dmc

try:
    load_dotenv(".env")
except Exception:
    pass


def _header(app: Dash):
    link_style = {
        "color": "#1a1a1a",
        "textDecoration": "none",
        "fontWeight": 600,
        "padding": "6px 10px",
        "borderRadius": "4px",
    }
    return dbc.Row(
        children=[
            dbc.Col(
                html.Img(
                    src=app.get_asset_url("Neuron23_Logo.png"),
                    style={"maxWidth": "130px", "height": "auto", "margin": "10px 16px"},
                ),
                width="auto",
                style={"borderRight": "1px solid #e0e0e0"},
            ),
            dbc.Col(
                html.Div(
                    [
                        html.Span(
                            dcc.Link("Home", href="/", style=link_style),
                            className="nav-link-wrapper",
                        ),
                        html.Span(
                            dcc.Link("Results Overview", href="/results", style=link_style),
                            className="nav-link-wrapper",
                        ),
                        html.Span(
                            dcc.Link("Biomarker Analysis", href="/analysis", style=link_style),
                            className="nav-link-wrapper",
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center", "height": "100%", "paddingLeft": "8px"},
                )
            ),
            dbc.Col(
                html.Span(
                    "PPMI LRRK2",
                    style={"color": "#888", "fontSize": "12px", "fontStyle": "italic"},
                ),
                className="ms-auto",
                width="auto",
                style={"display": "flex", "alignItems": "center", "paddingRight": "8px"},
            ),
        ],
        align="center",
        style={
            "backgroundColor": "#ffffff",
            "padding": "8px 16px",
            "borderBottom": "1px solid #e0e0e0",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
        },
    )


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], use_pages=True)
app.title = "Biomarker Dashboard"
init_cache(app.server)

_page_content = dbc.Container(
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

app.layout = html.Div(
    style={"minHeight": "100vh", "display": "flex", "flexDirection": "column"},
    children=[
        (
            dmc.MantineProvider(
                children=[_page_content],
                theme={"primaryColor": "blue"},
            )
            if dmc is not None
            else _page_content
        )
    ],
)


if __name__ == "__main__":
    # Dash's debug reloader spawns a child process; ensure env is visible there too.
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    app.run(debug=True)

