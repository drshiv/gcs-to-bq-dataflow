# GCS to BigQuery Dataflow Pipeline (Customer Data)

## 🌟 Overview

This project implements an Apache Beam pipeline designed to ingest customer data from a Google Cloud Storage (GCS) CSV file, apply data quality checks, remove duplicates, and load the clean data into a BigQuery table. The pipeline is configured to run on Google Cloud Dataflow.

### Key Pipeline Features

* **Input Data Source:** Reads CSV files from `gs://de-project-source/incoming/customers.csv`.
* **Output BigQuery Table:** Writes data to `de-project-479112:customer_data.customers`.
* **Deduplication:** Uses a tuple-based approach with `beam.Distinct()` to ensure only unique rows are loaded into BigQuery.

---

## 🛠️ Prerequisites and Setup

To deploy and run this pipeline, you must have the following Google Cloud resources and local environment set up:

### Google Cloud Resources

| Configuration | Value | Source |
| :--- | :--- | :--- |
| **Project ID** | `de-project-479112` | `dataflow_pipeline.py` |
| **Dataflow Region** | `us-central1` | `dataflow_pipeline.py` |
| **Staging Location** | `gs://de-project-source/staging/` | `dataflow_pipeline.py` |
| **Temp Location** | `gs://de-project-source/temp/` | `dataflow_pipeline.py` |
| **Output Table** | `de-project-479112:customer_data.customers` | `dataflow_pipeline.py` |

You must ensure that the necessary IAM roles (Dataflow Developer, BigQuery Data Editor, Storage Object Admin) are granted to your execution account.

### Local Environment Setup

1.  **Create and activate your Python virtual environment:**

    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # On Windows PowerShell
    source venv/bin/activate # On Linux/MacOS/Git Bash
    ```

2.  **Install dependencies:** The primary dependency is Apache Beam with GCP extensions.

    ```bash
    pip install -r requirements.txt
    ```

---

## 🔍 Data Validation and Cleansing Logic

The `clean_and_validate` function in the pipeline enforces the following data quality rules:

### Validation Rules (Record Filtering)

* **Customer ID (`customer_id`):** The record is discarded (`return None`) if the `customer_id` cannot be cast to an integer, is missing, or is less than or equal to zero.
* **Customer Name (`customer_name`):** The record is discarded if the field is empty or missing.
* **Email (`email`):** The record is discarded if the value does not match the standard email regular expression (`EMAIL_REGEX`).
* **Sign-up Date (`signup_date`):** The record is discarded if the value cannot be parsed using the expected format: `%Y-%m-%d`.

### Cleansing Transformations (Data Formatting)

For all records that pass validation, the following transformations are applied before loading to BigQuery:

* **Customer Name:** Formatted to **Title Case** (`record['customer_name'].title()`).
* **Email:** Formatted to **lowercase** (`record['email'].lower()`).

---

## 📝 BigQuery Target Schema

The destination table (`customers`) is expected to have the following schema definition:

| Field Name | Type | Mode |
| :--- | :--- | :--- |
| `customer_id` | INTEGER | REQUIRED |
| `customer_name` | STRING | NULLABLE |
| `email` | STRING | NULLABLE |
| `signup_date` | DATE | NULLABLE |

---

## 🚀 Pipeline Execution

### 1. Place Input File

Ensure the CSV input file is available in the GCS input location: `gs://de-project-source/incoming/customers.csv`.

### 2. Run the Pipeline

Execute the pipeline from your activated virtual environment:

```bash
python dataflow_pipeline.py