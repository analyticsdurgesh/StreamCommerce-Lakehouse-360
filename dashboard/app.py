from __future__ import annotations

import os
from datetime import datetime, timezone

from dash import Dash, Input, Output, dcc, html

from dashboard.pages import customer_analytics, data_quality_monitor, executive_overview, inventory_monitoring, order_revenue, product_performance

PAGES = {
    "/": ("Executive Overview", executive_overview.layout),
    "/revenue": ("Revenue & Orders", order_revenue.layout),
    "/customers": ("Customer Analytics", customer_analytics.layout),
    "/products": ("Product Performance", product_performance.layout),
    "/inventory": ("Inventory Monitoring", inventory_monitoring.layout),
    "/quality": ("Data Quality Monitor", data_quality_monitor.layout),
}

app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server


def nav():
    return html.Nav([dcc.Link(label, href=path, className="nav-link") for path, (label, _) in PAGES.items()], className="nav")


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        html.Header(
            [
                html.Div([html.H1("StreamCommerce Lakehouse 360"), html.P("Executive analytics powered by Gold-layer lakehouse tables")], className="brand"),
                html.Div(f"Last refresh: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", className="refresh"),
            ],
            className="topbar",
        ),
        nav(),
        html.Main(id="page-content"),
    ]
)


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def render(pathname: str):
    _, renderer = PAGES.get(pathname, PAGES["/"])
    return renderer()


if __name__ == "__main__":
    app.run_server(host=os.getenv("DASH_HOST", "0.0.0.0"), port=int(os.getenv("DASH_PORT", "8050")), debug=False)
