from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def layout():
    dq = read_gold_table("data_quality_scorecard")
    return html.Div(
        [
            html.Div("Data quality warnings appear when average pass rate drops below 95%.", className="notice"),
            dcc.Graph(figure=px.line(dq, x="checked_at", y="pass_percentage", color="table_name", markers=True, title="DQ Pass Rate Over Time")),
            dcc.Graph(figure=px.bar(dq, x="table_name", y="failed_record_count", color="check_name", title="Invalid Records by Source and Check")),
        ],
        className="page",
    )
