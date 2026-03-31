from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from astropy.stats import freedman_bin_width
from dash import Input, Output, State, callback, dcc, dash_table, html
import dash
import dash_bootstrap_components as dbc

from utils.biomarker_regression import run_biomarker_by_biomarker_cohort_regressions


dash.register_page(
    __name__,
    path="/analysis",
    name="Biomarker Analysis",
    title="Biomarker Analysis | Biomarker Dashboard",
)


DATA_PATH = Path("./data/processed/cleaned_biospecimen_analysis.csv")
PROJECTS_PATH = Path("./data/processed/cleaned_biospecimen_projects.csv")

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


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

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
        raise ValueError(f"Missing required columns in {path}: {sorted(missing)}")

    out = df.loc[
        :,
        [
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
        ],
    ].copy()

    out["TESTVALUE"] = pd.to_numeric(out["TESTVALUE"], errors="coerce")
    out["GBA"] = pd.to_numeric(out["GBA"], errors="coerce")
    out["AGE_AT_VISIT"] = pd.to_numeric(out["AGE_AT_VISIT"], errors="coerce")
    out["PROJECTID"] = pd.to_numeric(out["PROJECTID"], errors="coerce")
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


DF = load_data(DATA_PATH)
PROJECTS_DF = pd.read_csv(PROJECTS_PATH).copy()
PROJECTS_DF["PROJECTID"] = pd.to_numeric(PROJECTS_DF["PROJECTID"], errors="coerce")
PROJECTS_DF = PROJECTS_DF.dropna(subset=["PROJECTID"])
PROJECTS_DF["PROJECTID"] = PROJECTS_DF["PROJECTID"].astype(int)
PROJECTS_LOOKUP = PROJECTS_DF.set_index("PROJECTID")[["PI_NAME", "PI_INSTITUTION"]].to_dict(
    orient="index"
)

PROJECT_RUNDATES = (
    DF.groupby("PROJECTID", dropna=False)["RUNDATE"].agg(min_date="min", max_date="max").reset_index()
)
PROJECT_RUNDATES["PROJECTID"] = PROJECT_RUNDATES["PROJECTID"].astype(int)
PROJECT_RUNDATES_LOOKUP = PROJECT_RUNDATES.set_index("PROJECTID")[["min_date", "max_date"]].to_dict(
    orient="index"
)

TESTNAMES = sorted(DF["TESTNAME"].unique().tolist())
DEFAULT_TESTNAME = TESTNAMES[0] if TESTNAMES else None
COHORT_VALUES = [c for c in COHORTS["COHORT"]["Order"] if c in set(DF["COHORT"].unique())]


def description_card():
    return dbc.Card(
        id="description-card",
        children=[
            dbc.CardBody(
                [
                    html.H4("Welcome to the Neuron23 LRRK2 Biomarker Dashboard"),
                    html.Div(
                        id="intro",
                        children=(
                            "Explore biomarker distributions by cohort. Click on the dropdown to select a biomarker."
                        ),
                        style={"marginTop": "8px"},
                    ),
                ]
            )
        ],
    )


def generate_control_card():
    biomarker_control_card = dbc.Card(
        children=[
            dbc.CardBody(
                [
                    html.H5("Select Biomarker", className="card-title"),
                    dcc.Dropdown(
                        id="testname",
                        options=[{"label": t, "value": t} for t in TESTNAMES],
                        value=DEFAULT_TESTNAME,
                        clearable=False,
                        placeholder="No biomarkers found in TESTNAME",
                    ),
                ]
            )
        ],
    )

    cohort_control_card = dbc.Card(
        children=[
            dbc.CardBody(
                [
                    html.H5("Select cohorts", className="card-title"),
                    html.P("Compare groups by", className="card-title"),
                    dbc.RadioItems(
                        id="groupby",
                        options=[
                            {"label": "Classifier result", "value": "HEURISTIC"},
                            {"label": "PD Diagnosis", "value": "COHORT"},
                        ],
                        value="HEURISTIC",
                        inline=False,
                    ),
                    html.Hr(),
                    html.P("Cohorts to include", className="card-title"),
                    dbc.Checklist(
                        id="cohort_filter",
                        options=[{"label": c, "value": c} for c in COHORT_VALUES],
                        value=COHORT_VALUES,
                        inline=False,
                    ),
                    html.Hr(),
                    html.P("GBA carriers", className="card-title"),
                    dbc.RadioItems(
                        id="gba_filter_mode",
                        options=[
                            {"label": "Included", "value": "included"},
                            {"label": "Excluded", "value": "excluded"},
                        ],
                        value="included",
                        inline=False,
                    ),
                ]
            )
        ],
    )

    data_control_card = dbc.Card(
        children=[
            dbc.CardBody(
                [
                    html.H5("Adjust data", className="card-title"),
                    html.P("Data transformation", className="card-title"),
                    dbc.RadioItems(
                        id="transform",
                        options=[
                            {"label": "None", "value": "none"},
                            {"label": "log", "value": "log"},
                        ],
                        value="log",
                        inline=True,
                    ),
                ]
            )
        ],
    )

    return html.Div(
        id="control-cards",
        children=[
            biomarker_control_card,
            html.Div(style={"height": "12px"}),
            cohort_control_card,
            html.Div(style={"height": "12px"}),
            data_control_card,
        ],
    )


def _empty_figure(title: str):
    fig = px.scatter(title=title)
    fig.update_layout(template="plotly_white")
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


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
) -> pd.DataFrame:
    selected_cohorts = cohort_filter or []
    dff = DF[(DF["TESTNAME"] == testname) & (DF["COHORT"].isin(selected_cohorts))]
    if gba_filter_mode == "excluded":
        dff = dff[dff["GBA"] != 1]
    return dff


def _build_model_df(
    testname: str,
    groupby: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    transform: str | None,
) -> tuple[pd.DataFrame, str, str]:
    group_col = groupby if groupby in {"COHORT", "HEURISTIC"} else "COHORT"
    model_df = _filtered_df(testname, cohort_filter, gba_filter_mode).copy()
    if transform == "log":
        model_df = model_df[model_df["TESTVALUE"] > 0].copy()
        model_df["LOG_TESTVALUE"] = np.log(model_df["TESTVALUE"])
        testvalue_col = "LOG_TESTVALUE"
    else:
        testvalue_col = "TESTVALUE"
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
    if not testname:
        return "", ""

    s = DF.loc[DF["TESTNAME"] == testname, "PROJECTID"]
    project_id = None
    if not s.empty:
        try:
            project_id = int(pd.to_numeric(s, errors="coerce").dropna().mode().iloc[0])
        except Exception:
            project_id = None

    meta_children: list = []
    if project_id is None:
        meta_children = [html.Div("Project ID: (unknown)")]
        return str(testname), meta_children

    meta_children.append(html.Div(f"Project ID: {project_id}"))
    info = PROJECTS_LOOKUP.get(project_id)
    if info:
        meta_children.append(html.Div(f"PI Name: {info.get('PI_NAME', '')}"))
        meta_children.append(html.Div(f"PI Institution: {info.get('PI_INSTITUTION', '')}"))
    else:
        meta_children.append(html.Div("PI Name: (unknown)"))
        meta_children.append(html.Div("PI Institution: (unknown)"))

    date_info = PROJECT_RUNDATES_LOOKUP.get(project_id)
    if date_info and date_info.get("min_date") is not None and date_info.get("max_date") is not None:
        min_d = pd.to_datetime(date_info["min_date"]).date().isoformat()
        max_d = pd.to_datetime(date_info["max_date"]).date().isoformat()
        meta_children.append(html.Div(f"Run dates: {min_d} to {max_d}"))
    else:
        meta_children.append(html.Div("Run dates: (unknown)"))

    return str(testname), meta_children


layout = html.Div(
    style={"minHeight": "75vh", "display": "flex", "flexDirection": "column"},
    children=[
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
                        "backgroundColor": "#FFFFFF",
                        "padding": "8px 8px",
                        "height": "100%",
                    },
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                html.H3(id="biomarker-title", style={"margin": "0"}),
                                html.Div(
                                    id="biomarker-meta",
                                    style={"marginTop": "6px", "color": "#444"},
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
                        html.Div(id="stats-results", style={"marginTop": "14px"}),
                        html.Div(
                            [
                                html.Div(style={"height": "24px"}),
                                dbc.Button(
                                    "Download data as a CSV",
                                    id="download-data-btn",
                                    color="primary",
                                    size="sm",
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
    [Output("hist", "figure"), Output("box", "figure")],
    [
        Input("testname", "value"),
        Input("groupby", "value"),
        Input("cohort_filter", "value"),
        Input("gba_filter_mode", "value"),
        Input("transform", "value"),
    ],
)
def update_figures(
    testname: str | None,
    groupby: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    transform: str | None,
):
    if not testname:
        empty = _empty_figure("No data to display")
        return empty, empty

    selected_cohorts = cohort_filter or []
    dff = _filtered_df(testname, cohort_filter, gba_filter_mode)
    if dff.empty:
        empty = _empty_figure("No rows match current filters")
        return empty, empty

    unit = dff["UNITS"].unique()[0]
    x_col = "TESTVALUE"
    x_label = f"{testname} [{unit}]"
    if transform == "log":
        dff = dff[dff["TESTVALUE"] > 0].copy()
        if dff.empty:
            empty = _empty_figure("No rows match current filters")
            return empty, empty
        dff["LOG_TESTVALUE"] = np.log(dff["TESTVALUE"])
        x_col = "LOG_TESTVALUE"
        x_label = f"log({testname}) [{unit}]"

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

    dist_fig = ff.create_distplot(
        hist_data=hist_data,
        group_labels=hist_labels,
        colors=colors,
        show_hist=True,
        show_rug=False,
        bin_size=freedman_bin_width(dff[x_col]),
    )
    dist_fig.update_layout(
        template="plotly_white",
        title="Histogram",
        xaxis_title=x_label,
        legend_title_text=group_col,
        margin=dict(l=40, r=20, t=60, b=40),
    )

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
        Input("testname", "value"),
        Input("groupby", "value"),
        Input("cohort_filter", "value"),
        Input("gba_filter_mode", "value"),
        Input("transform", "value"),
    ],
)
def update_stats_table(
    testname: str | None,
    groupby: str | None,
    cohort_filter: list[str] | None,
    gba_filter_mode: str | None,
    transform: str | None,
):
    if not testname:
        return html.Div("Select a biomarker to see cohort regression results.")

    model_df, group_col, testvalue_col = _build_model_df(
        testname, groupby, cohort_filter, gba_filter_mode, transform
    )
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
        omnibus_df, pairwise_df = run_biomarker_by_biomarker_cohort_regressions(
            model_df,
            standardize_within_biomarker=True,
            standardize_outcome_for_beta=True,
            cohort_col=group_col,
            cohort_categories=cohort_categories,
            testvalue_col=testvalue_col,
            z_col=z_col,
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
):
    if not testname:
        return None

    model_df, group_col, testvalue_col = _build_model_df(
        testname, groupby, cohort_filter, gba_filter_mode, transform
    )
    if model_df.empty:
        return None

    cols = [
        "PATNO",
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

    safe_name = str(testname).replace("/", "-").replace("\\", "-")
    filename = f"{safe_name}__groupby-{group_col}__transform-{transform or 'none'}.csv"
    return dcc.send_data_frame(export_df.to_csv, filename, index=False)

