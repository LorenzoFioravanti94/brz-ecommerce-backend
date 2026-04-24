# 🛒 Olist E-Commerce Data Pipeline

A end-to-end **data engineering portfolio project** built with **dbt Core** and **DuckDB**, modelling the public Brazilian e-commerce dataset by Olist following the **Medallion Architecture** (Bronze → Silver → Gold) with a **Snowflake dimensional model** in the Gold layer.

---

## 📌 Project Overview

This project simulates a real-world data pipeline for an e-commerce company. Starting from raw transactional CSV data, it progressively transforms and models the data through three layers until reaching a clean, analytics-ready dimensional model suitable for reporting and downstream machine learning use cases.

**Key design decisions:**
- **Medallion Architecture** for clear separation of concerns across pipeline layers
- **Snowflake Schema** (not Star Schema) in the Gold layer to minimise redundancy
- **DuckDB** as the local analytical database engine — no cloud platform required
- **dbt Core** (open-source) for all data transformations and pipeline orchestration
- A dedicated Python ingestion script loads raw CSVs into DuckDB before dbt runs

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| [dbt Core](https://docs.getdbt.com/) | Data transformation & pipeline orchestration |
| [DuckDB](https://duckdb.org/) | Local analytical database engine |
| [Python 3.10+](https://www.python.org/) | Raw data ingestion script |
| [DBeaver](https://dbeaver.io/) | SQL exploration & query interface |
| Git & GitHub | Version control |

---

## 🏗️ Architecture

```
Raw CSV files
     │
     ▼
[scripts/load_raw.py]  ← Python ingestion script
     │
     ▼
DuckDB — schema: raw   ← Source layer (declared in sources.yml, no SQL models)
     │
     ▼
DuckDB — schema: bronze  ← Exact copies of source tables, lightly typed
     │
     ▼
DuckDB — schema: silver  ← Cleaned, joined, business logic applied
     │
     ▼
DuckDB — schema: gold    ← Snowflake dimensional model (fact + dimension tables)
```

All four schemas live inside a **single DuckDB file** (`data/duckdb/olist.duckdb`).

---

## 📁 Repository Structure

```
olist-data-pipeline/
│
├── data/                          # ⚠️ gitignored — not committed to the repo
│   ├── raw/                       # Olist CSV files (download instructions below)
│   └── duckdb/                    # olist.duckdb — generated automatically
│
├── scripts/
│   └── load_raw.py                # Ingestion script: loads CSVs into DuckDB raw schema
│
├── olist_dbt/                     # dbt Core project
│   ├── models/
│   │   ├── sources.yml            # Source layer — declarative YAML only, no SQL
│   │   ├── bronze/                # Bronze layer models
│   │   ├── silver/                # Silver layer models
│   │   └── gold/                  # Gold layer — Snowflake dimensional model
│   ├── dbt_project.yml
│   └── profiles.yml               # DuckDB connection config
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🗂️ Dimensional Model (Gold Layer — Snowflake Schema)

The Gold layer exposes a Snowflake Schema optimised for analytics:

```
fact_order_items
      │
      ├──► dim_orders ──► dim_customers ──► dim_geography
      │
      ├──► dim_products ──► dim_categories
      │
      ├──► dim_sellers ──► dim_geography
      │
      └──► dim_payments
```

The Snowflake Schema was chosen over the Star Schema to **avoid data redundancy**, particularly for shared dimensions like geography (used by both customers and sellers).

---

## ⚙️ How to Run This Project Locally

### Prerequisites

- Python 3.10 or higher
- Git

### 1. Clone the repository

```bash
git clone https://github.com/LorenzoFioravanti94/olist-data-pipeline.git
cd olist-data-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # macOS / Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download the raw data

Download the **Brazilian E-Commerce Public Dataset by Olist** from Kaggle:

🔗 https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Place all CSV files inside the `data/raw/` directory. The expected files are:

```
data/raw/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_customers_dataset.csv
├── olist_sellers_dataset.csv
├── olist_products_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

### 5. Load raw data into DuckDB

```bash
python scripts/load_raw.py
```

This script creates the `data/duckdb/olist.duckdb` file and loads all CSVs into the `raw` schema.

### 6. Run the dbt pipeline

```bash
cd olist_dbt
dbt deps
dbt run
dbt test
```

### 7. Explore the data

Open DBeaver, create a new **DuckDB connection**, and point it to `data/duckdb/olist.duckdb`. You will find all four schemas (`raw`, `bronze`, `silver`, `gold`) populated and ready to query.

---

## 📊 Dataset

**Brazilian E-Commerce Public Dataset by Olist**
- Source: [Kaggle — olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Publisher: [Olist](https://olist.com/)
- License: [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

### License terms summary

This dataset is made available under the **CC BY-NC-SA 4.0** license. This means:

- ✅ You are free to **share** and **adapt** the data
- ✅ Attribution to the original publisher (Olist) is provided above
- ❌ The data **may not be used for commercial purposes**
- ❌ Derived works must be shared under the **same license**

In compliance with these terms:
- The raw CSV files are **not included** in this repository
- Instructions to download the data directly from the original Kaggle source are provided above
- This project is strictly **non-commercial** and intended for educational and portfolio purposes only

---

## 📄 Code License

The code in this repository (dbt models, Python scripts, configuration files) is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

Note: The MIT License applies to the **code only**. The underlying dataset is subject to its own CC BY-NC-SA 4.0 license as described above.

---

## 👤 Author

**Lorenzo Fioravanti**
Junior Data Engineer | Barcelona, Spain
[LinkedIn](https://www.linkedin.com/in/lorenzo-fioravanti-177ba7303/) · [GitHub](https://github.com/LorenzoFioravanti94)
