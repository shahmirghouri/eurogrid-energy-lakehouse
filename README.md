# EuroGrid Energy Intelligence Lakehouse

![CI Status](https://github.com/shahmirghouri/eurogrid-energy-lakehouse/actions/workflows/ci.yml/badge.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue)
![Airflow 2.8](https://img.shields.io/badge/airflow-2.8-green)
![dbt](https://img.shields.io/badge/dbt-1.7-orange)

## What this project does

A production-grade data engineering pipeline that ingests real-time European 
electricity market data from the ENTSO-E Transparency Platform — covering 
Belgium, Germany, France, and the Netherlands.

The pipeline detects negative electricity prices (when renewables overproduce),
tracks cross-border price spreads, and delivers daily market summaries to 
a Power BI dashboard used for energy market analysis.

## Technical Architecture

The project implements a Medallion Architecture using a modern data stack:

![Lineage Graph](./asset/architecture_lineage.png)

1. **Ingestion**: Python/Airflow (Bronze)
2. **Processing**: PySpark (Silver)
3. **Modeling**: dbt & DuckDB (Gold)

```
ENTSO-E API → [Airflow DAG] → Bronze Layer → [PySpark] → Silver Layer → [dbt] → Gold Layer → Power BI
                (daily 07:00)   (raw Parquet)              (clean Parquet)        (DuckDB)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, entsoe-py |
| Orchestration | Apache Airflow 2.8 |
| Processing | PySpark 3.5, Delta Lake |
| Transformation | dbt-core 1.7, DuckDB |
| Data Quality | Great Expectations |
| Infrastructure | Docker, GitHub Actions |
| Visualisation | Power BI |

## Quick Start (3 commands)

```bash
git clone https://github.com/yourusername/eurogrid-energy-lakehouse.git
cd eurogrid-energy-lakehouse
cp .env.example .env  # add your ENTSO-E API key
docker-compose up -d  # starts Airflow at localhost:8080
```

## Key Findings (May 5 – May 8, 2026)

- **Market Price Spread:** France remains the most competitive market in the region with an average price of **86.24 EUR/MWh**, while German prices (**124.48 EUR/MWh**) are **44% higher** on average.
- **Regional Correlation:** Prices in Belgium (**116.29 EUR/MWh**) and the Netherlands (**118.83 EUR/MWh**) show high correlation, reflecting the strong physical interconnection between these two grid zones.
- **Negative Price Stability:** No negative price events were recorded during this 4-day weekday window, indicating stable demand-supply balance across the Central Western Europe (CWE) region.

## Project structure

```
├── ingestion/      Python ENTSO-E API client
├── transform/      PySpark Silver transformation jobs
├── dags/           Apache Airflow DAG definitions  
├── dbt_project/    dbt Gold models (staging + marts)
├── tests/          Data quality tests
└── docs/           Architecture diagrams
```


## Author

Muhammad Ghouri — MSc Data Science, Hasselt University  
[LinkedIn](https://linkedin.com/in/muhammadghouri) | [Email](mailto:shahmir.ghouri99@gmail.com)
