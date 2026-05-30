from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def layout():
    revenue = read_gold_table("daily_revenue")
    payments = read_gold_table("payment_reliability")
    return html.Div(
        [
            dcc.Graph(figure=px.bar(revenue, x="event_date", y="total_orders", title="Orders Trend")),
            dcc.Graph(figure=px.line(revenue, x="event_date", y="average_order_value", markers=True, title="Average Order Value")),
            dcc.Graph(figure=px.area(payments, x="event_date", y="failed_payment_rate", title="Failed Payment Rate")),
        ],
        className="page two-col",
    )
