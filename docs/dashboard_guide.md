# Dashboard Guide

Run:

```bash
make dashboard
```

Open http://localhost:8050.

Pages:

- Executive Overview: top KPIs, revenue trend, top products.
- Revenue & Orders: order trend, AOV, failed payment rate.
- Customer Analytics: lifetime value and repeat purchase behavior.
- Product Performance: category revenue and product mix.
- Inventory Monitoring: stockout risk.
- Data Quality Monitor: pass-rate trend and invalid records by source.

The app reads Gold tables from `GOLD_DATA_PATH`, defaulting to `data/gold`. If no pipeline output exists yet, it displays sample data so the dashboard can be reviewed immediately.
