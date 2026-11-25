# E-Commerce Batch Data Engineering Pipeline

## Project Overview
Developed an end-to-end batch data engineering pipeline using Databricks to process e-commerce data. The project follows the Medallion Architecture (Bronze, Silver, and Gold) to ingest, clean, transform, and prepare data for business reporting.

## Business Objective
To build a reliable data pipeline that processes e-commerce transactions and generates business-ready datasets for analyzing sales, customer activity, order trends, and data quality.

## Technologies Used
- Databricks
- PySpark
- Python
- Spark SQL
- Delta Lake
- Auto Loader
- Lakeflow Spark Declarative Pipelines
- Lakeflow Jobs
- GitHub

## Architecture

Raw CSV Files → Bronze Layer → Silver Layer → Gold Layer → Business Reporting

### 1. Bronze Layer
- Ingested raw e-commerce CSV files using Databricks Auto Loader.
- Added ingestion timestamp and source file metadata.
- Created Bronze tables for orders, order items, products, and customers.

### 2. Silver Layer
- Cleaned string columns using trimming.
- Handled missing and invalid records.
- Converted columns into appropriate data types.
- Removed duplicate records.
- Created cleaned Silver tables.

### 3. Gold Layer
Created business-ready tables for:
- Product performance analysis.
- Monthly order trends.
- Customer analysis.
- Order status analysis.
- Data quality reporting.
- Order item quality validation.
- Consolidated data quality reporting.

### 4. Data Quality
Implemented validation checks for:
- Missing order IDs.
- Duplicate order IDs.
- Missing order status.
- Missing product IDs.
- Invalid product prices.
- Invalid freight values.

### 5. Scheduling and Orchestration
Configured a daily scheduled execution using Databricks Lakeflow Jobs to automate pipeline runs.

## Dataset
The project uses the Brazilian E-Commerce Public Dataset by Olist.

Dataset source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Key Outcomes
- Built a multi-layer batch data pipeline.
- Automated ingestion using Auto Loader.
- Implemented data cleaning and transformation.
- Created analytical Gold tables.
- Added data quality validation.
- Configured scheduled pipeline execution.

## Future Enhancements
- Integrate external REST APIs.
- Build a separate streaming data pipeline.
- Create business dashboards using Power BI.
- Add pipeline failure notifications and monitoring.
