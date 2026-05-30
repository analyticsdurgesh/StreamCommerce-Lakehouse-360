from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def layout():
    products = read_gold_table("product_performance")
    return html.Div(
        [
            dcc.Graph(figure=px.bar(products, x="category", y="revenue", color="category", title="Revenue by Category")),
            dcc.Graph(figure=px.treemap(products, path=["category", "product_id"], values="revenue", title="Product Performance Mix")),
        ],
        className="page two-col",
    )
