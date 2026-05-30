from __future__ import annotations

from dash import dcc, html
import plotly.express as px

from dashboard.data import read_gold_table


def kpi(label: str, value: str, tone: str = "ok") -> html.Div:
    return html.Div([html.Div(label, className="kpi-label"), html.Div(value, className=f"kpi-value {tone}")], className="kpi-card")


def layout():
    revenue = read_gold_table("daily_revenue")
    dq = read_gold_table("data_quality_scorecard")
    total_revenue = revenue["total_revenue"].sum() if "total_revenue" in revenue else 0
    total_orders = revenue["total_orders"].sum() if "total_orders" in revenue else 0
    dq_rate = dq["pass_percentage"].mean() if "pass_percentage" in dq else 1
    return html.Div(
        [
            html.Div(
                [
                    kpi("Total revenue", f"${total_revenue:,.0f}"),
                    kpi("Total orders", f"{total_orders:,.0f}"),
                    kpi("Avg order value", f"${revenue['average_order_value'].mean():,.2f}" if "average_order_value" in revenue else "$0"),
                    kpi("DQ pass rate", f"{dq_rate:.1%}", "warn" if dq_rate < 0.95 else "ok"),
                ],
                className="kpi-grid",
            ),
            dcc.Graph(figure=px.line(revenue, x="event_date", y="total_revenue", markers=True, title="Revenue Trend")),
            dcc.Graph(figure=px.bar(read_gold_table("product_performance").head(10), x="product_id", y="revenue", color="category", title="Top Products by Revenue")),
        ],
        className="page",
    )
