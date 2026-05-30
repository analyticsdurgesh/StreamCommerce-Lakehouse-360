# Medallion Design

Bronze is append-first and intentionally permissive. It is optimized for auditability and replay.

Silver is trust-building. It handles schema normalization, deduplication, late event handling, required field validation, type casting, and data quality flags.

Gold is consumption-ready. It exposes tables aligned to business questions: daily revenue, product performance, customer 360, inventory health, payment reliability, and data quality scorecard.
