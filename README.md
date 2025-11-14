# E-Commerce Data Exercise

This mini project demonstrates generating synthetic e-commerce CSV datasets for a small online store, loading them into a SQLite database, and running analytical queries on top of the ingested data. It showcases basic data engineering steps: synthetic data creation, schema definition, ingestion, and reporting.

## Repository Contents

- `categories.csv` – seed data for 10 product categories.
- `users.csv` – 100 synthetic customer accounts with creation dates.
- `products.csv` – 200 products mapped to categories with prices.
- `orders.csv` – 500 user orders including status and timestamps.
- `order_items.csv` – 1,500 line items connecting orders to products.
- `generate_data.py` – script used to generate all CSV datasets.
- `ingest.py` – script that creates `ecommerce.db` and loads the CSVs with referential integrity enforced.
- `ecommerce.db` – SQLite database produced by running `ingest.py`, containing the populated tables.

