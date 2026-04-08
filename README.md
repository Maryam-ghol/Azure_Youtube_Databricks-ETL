# YouTube Databricks ETL

This project demonstrates a **professional ETL pipeline** for YouTube Analytics using **Azure Databricks** and **Unity Catalog**. It is designed to be **cost-efficient** and showcase modern data engineering best practices.

## Project Structure

```text
youtube-databricks-etl/
│
├── notebooks/
│   ├── 01_setup.ipynb             # Cluster and Spark environment & schema setup
│   ├── 02_bronze_ingestion.ipynb # Catalog  setup
│
├── src/
│   ├── extract.py                  # YouTube API ingestion
│   ├── transform.py                # Bronze → Silver transformations
│   ├── load.py                     # Silver → Gold loading
│
├── jobs/
│   └── etl_job_config.json         # Databricks job orchestration
│
├── tests/
│   └── test_extract.py             # Unit tests for ETL code
│
└── README.md
