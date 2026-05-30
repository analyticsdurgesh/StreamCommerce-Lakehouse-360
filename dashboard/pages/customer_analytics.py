from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def layout():
    customers = read_gold_table("customer_360")
    return html.Div(
        [
            dcc.Graph(figure=px.histogram(customers, x="lifetime_value", nbins=12, title="Customer Lifetime Value Distribution")),
            dcc.Graph(figure=px.scatter(customers, x="sessions", y="lifetime_value", size="orders", color="repeat_purchase_flag", title="Engagement vs Lifetime Value")),
        ],
        className="page two-col",
    )
