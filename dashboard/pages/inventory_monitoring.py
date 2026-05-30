from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def layout():
    inv = read_gold_table("inventory_health").sort_values("stockout_risk", ascending=False)
    return html.Div(
        [
            dcc.Graph(figure=px.bar(inv, x="product_id", y="stockout_risk", color="stockout_warnings", title="Inventory Stockout Risk")),
            dcc.Graph(figure=px.scatter(inv, x="stock_events", y="stockout_warnings", size="stockout_risk", hover_name="product_id", title="Inventory Event Health")),
        ],
        className="page two-col",
    )
