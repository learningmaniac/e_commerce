# E-Commerce Analytics Pipeline

### Snowflake · dbt · Power BI

## Project Overview

This project builds a full end-to-end data engineering pipeline for an e-commerce business using the Brazilian E-Commerce dataset (Olist) from Kaggle. Raw CSV files are loaded into Snowflake, transformed using dbt into a star schema, and visualised in Power BI for business analysis.

The pipeline answers real business questions:

- Which Brazilian state generates the most revenue?
- How is the delivery team performing — what percentage of orders are delivered vs canceled?
- Which months drive the highest sales — where should the business focus its marketing?

---

## Architecture

```
CSV Files (Kaggle)
      ↓
Snowflake — RAW schema
      ↓
dbt Staging Models
(stg__orders · stg__customers · stg__order_payments)
      ↓
dbt Mart Models
(fct_orders · dim_customers)
      ↓
Power BI Dashboard
```

---

## Tools & Technologies

| Tool      | Purpose                                                |
| --------- | ------------------------------------------------------ |
| Snowflake | Cloud data warehouse — stores raw and transformed data |
| dbt       | Data transformation — builds staging and mart models   |
| Power BI  | Business intelligence — dashboard and visualisations   |
| GitHub    | Version control — project documentation and code       |

---

## Data Pipeline

### 1. Ingestion

Raw CSV files from the Olist Brazilian E-Commerce dataset are loaded into Snowflake's RAW schema using the `COPY INTO` command.

**Source tables:**

- `RAW.ORDERS` — 99,441 orders from 2016 to 2018
- `RAW.CUSTOMERS` — customer location and identity data
- `RAW.ORDER_PAYMENTS` — payment method and value per order

### 2. Staging (dbt)

Staging models clean and rename raw columns with no business logic applied. Source tables are referenced using dbt's `source()` macro defined in `_src_e-commerce.yml`.

- `stg__orders` — renames timestamps, standardises column names
- `stg__customers` — selects and cleans customer fields
- `stg__order_payments` — renames `payment_type` to `payment_method`

### 3. Marts (dbt)

**`fct_orders`** — one row per order (fact table)

Contains measurable event-level data: order status, dates, payment aggregations. Payment values are aggregated using `SUM`, `MIN`, `MAX`, `AVG` per `order_id` to maintain a clean single-row-per-order grain.

Key columns: `order_id`, `customer_id`, `order_status`, `order_purchase_date`, `delivery_days`, `total_payment_values`, `payment_methods`

**`dim_customers`** — one row per customer (dimension table)

Contains customer descriptions plus pre-computed lifetime metrics, avoiding expensive aggregations at query time.

Key columns: `customer_id`, `customer_city`, `customer_state`, `total_orders`, `lifetime_payment_value`, `total_delivered_orders`, `total_canceled_orders`, `first_order_date`, `last_order_date`

### 4. Data Quality (dbt Tests)

All mart models have dbt tests defined in `schema.yml`:

- `unique` and `not_null` on all primary keys
- `accepted_values` on `order_status`
- `relationships` test — every `customer_id` in `fct_orders` must exist in `dim_customers`

All tests pass on the full dataset of 99,441 orders.

---

## Key Concepts Learned

**Fact vs Dimension modeling** — Fact tables store events (what happened), not just numeric columns. They can contain descriptive columns about the event itself like `order_status`. Dimension tables store context about objects (who, what, where) and can contain pre-computed numeric metrics like `lifetime_payment_value` to avoid expensive query-time aggregations.

**Table grain** — The grain of `fct_orders` is one row per `order_id`. Since one order can have multiple payment rows with different payment methods, payment data is pre-aggregated in a CTE before joining — maintaining clean grain in the final model.

**dbt lineage (DAG)** — The `ref()` function is not just a table reference — it tells dbt the dependency order of models and automatically builds the full pipeline DAG.

---

## Power BI Dashboard

Three visuals built on top of the dbt mart models:

**1. Monthly Revenue Trend (Line Chart)**
Shows distribution of total payment value across months. Helps identify peak sales months and seasonal patterns for marketing targeting.

**2. Revenue by State (Bar Chart — Top 10)**
Shows which Brazilian states generate the most lifetime revenue. Helps the business prioritise regional marketing and logistics investment.

**3. Order Status Breakdown (Donut Chart)**
Shows the split of orders across delivered, canceled, shipped, and other statuses. Directly reflects delivery team performance and operational health.

---

## Project Structure

```
e_commerce/
│
├── models/
│   ├── staging/
│   │   ├── _src_e-commerce.yml
│   │   ├── stg__orders.sql
│   │   ├── stg__customers.sql
│   │   └── stg__order_payments.sql
│   │
│   └── marts/
│       ├── schema.yml
│       ├── fct_orders.sql
│       └── dim_customers.sql
│
├── dbt_project.yml
└── README.md
```

---

## How to Run

**Prerequisites:** Snowflake account, dbt-snowflake installed, Power BI Desktop

**1. Load raw data into Snowflake**

```sql
-- Create database and schemas
CREATE DATABASE e_commerce;
CREATE SCHEMA e_commerce.raw;
CREATE SCHEMA e_commerce.transform;

-- Load CSVs using COPY INTO command
```

**2. Run dbt models**

```bash
dbt run
dbt test
```

**3. Generate and view dbt docs**

```bash
dbt docs generate
dbt docs serve --port 8001
```

**4. Connect Power BI**
Open Power BI Desktop → Get Data → Snowflake → connect to `E_COMMERCE.TRANSFORM`

---

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

- 99,441 orders · 2016–2018 · Multiple Brazilian marketplaces
