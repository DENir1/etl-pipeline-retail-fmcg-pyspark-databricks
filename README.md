
# 🛒 FMCG Data Engineering & Analytics Platform

**End-to-end FMCG ETL pipeline using AWS S3, Databricks, PySpark, Spark SQL and Delta Lake with full and incremental data processing.**

_An end-to-end data engineering project demonstrating cloud-based ingestion, ETL processing, dimension and fact processing, Delta Lake operations, incremental loading, data-quality validation, and interactive analytics using a Databricks SQL Dashboard._

---

## 📌 Table of Contents

- <a ref="#Overview">Overview</a>
- <a ref="#Project Architecture">Project Architecture</a>
- <a ref="#Business Problem">Business Problem</a>
- <a ref="#Data Source">Data Source</a>
- <a ref="#Tools & Technologies">Tools & Technologies</a>
- <a ref="#AWS S3 Data Ingestion">AWS S3 Data Ingestion</a>
- <a ref="#ETL Data Processing">ETL Data Processing</a>
- <a ref="#Bronze Layer">Bronze Layer</a>
- <a ref="#Silver Layer">Silver Layer</a>
- <a ref="#Gold Layer">Gold Layer</a>
- <a ref="#Dimension Processing">Dimension Processing</a>
- <a ref="#Fact Processing">Fact Processing</a>
- <a ref="#Full Load">Full Load</a>
- <a ref="#Incremental Load">Incremental Load</a>
- <a ref="#Delta Lake">Delta Lake</a>
- <a ref="#Data Quality">Data Quality</a>
- <a ref="#Dashboard & Analytics">Dashboard & Analytics</a>
- <a ref="#ETL Job">ETL Job</a>
- <a ref="#Project Structure">Project Structure</a>
- <a ref="#Key Features">Key Features</a>
- <a ref="#How to Run">How to Run</a>
- <a ref="#Future Enhancements">Future Enhancements</a>
- <a ref="#Author & Contact">Author Highlights</a>

---

## 📖 Overview

This project demonstrates an end-to-end **FMCG data engineering and analytics solution** built using **AWS S3, Databricks, PySpark, Spark SQL and Delta Lake**.

Source files are uploaded to AWS S3 and processed through a structured ETL pipeline in Databricks. Data flows through **Bronze, Silver and Gold layers**, where it is ingested, cleansed, transformed and prepared for analytics.

The project includes separate **dimension and fact processing**, **full-load and incremental-load pipelines**, Delta Lake `MERGE` operations, data-quality checks, and an interactive Databricks SQL dashboard.

---

## 🏗️ Project Architecture

```text
                           AWS S3
                             │
                             ▼
                    ┌─────────────────┐
                    │  Landing Zone   │
                    │    /landing     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Databricks   │
                    │  PySpark / SQL  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Bronze Layer   │
                    │  Raw / Ingested │
                    │      Data       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Silver Layer   │
                    │ Cleaned /       │
                    │ Transformed     │
                    │      Data       │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
          ┌────────────────┐  ┌────────────────┐
          │   Dimensions   │  │      Facts     │
          │ Customer       │  │ Full Load      │
          │ Product        │  │ Incremental    │
          │ Pricing        │  │ Load           │
          │ Date           │  │                │
          └────────┬───────┘  └────────┬───────┘
                   │                   │
                   └──────────┬────────┘
                              ▼
                    ┌─────────────────┐
                    │   Gold Layer    │
                    │ Business-ready  │
                    │      Data       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Enriched Gold  │
                    │      View       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Databricks SQL  │
                    │    Dashboard    │
                    └─────────────────┘

        Successfully processed files
        S3 /landing/  ─────────────►  S3 /processed/

```

---

## 💼 Business Problem

This project addresses:

- Cloud-based source-file ingestion
- Landing and processed file management
- Data cleansing and standardization
- Dimension and fact data processing
- Full and incremental processing
- Duplicate and invalid-record handling
- Business-ready Gold-layer datasets
- Interactive KPI and business-performance reporting

---

## 📂 Data Source

Source files are stored in an AWS S3 bucket.

S3 Folder Structure

```text

s3://child-sportsbar/customers/

│
├── customer/
│   ├── landing/            # Raw incoming customer files
│   └── processed/          # Archived / successfully processed customer files
│
├── products/
│   ├── landing/            # Raw incoming product catalog files
│   └── processed/          # Archived / successfully processed product files
│
├── pricing/
│   ├── landing/            # Raw incoming pricing tier updates
│   └── processed/          # Archived / successfully processed pricing files
│
└── orders/
    ├── landing/            # Raw incoming order transaction updates
    └── processed/          # Archived / successfully processed orders files


File Processing Workflow:-

- Source files are placed in the S3 landing folder.
- Databricks reads the files using PySpark.
- Data is loaded into the ETL pipeline.
- Data is processed through the required transformation layers.
- Successfully processed files are moved from landing to processed.
- The landing-to-processed workflow helps control file-level processing and reprocessing.

```
---

## 📂 Tools & Technologies

| Technology | Purpose |
| :--- | :--- |
| **AWS S3** | Cloud-based source file storage |
| **Databricks** | Data engineering and processing platform |
| **PySpark** | Distributed data transformation and processing |
| **Spark SQL** | SQL-based transformation and analytics |
| **Delta Lake** | Reliable transactional storage |
| **Delta Tables** | Curated analytical datasets |
| **SQL** | Validation, transformation and analytics |
| **Databricks SQL Dashboard** | Interactive reporting and visualization |

---

## 🔄 ETL Data Processing

The overall processing flow is:

```
Source
  ↓
S3 Landing
  ↓
Bronze
  ↓
Silver
  ↓
Dimensions + Facts
  ↓
Gold
  ↓
Enriched Analytical View
  ↓
Dashboard
```

The transformation process includes:

- Data ingestion
- Data-type conversion
- Data cleansing
- String standardization
- Date standardization
- Invalid-value handling
- Duplicate handling
- Joins
- Aggregations
- Dimension processing
- Fact processing
- Data-quality validation
- Incremental processing
- Full Load processing

---

## 🥉 Bronze Layer

The Bronze layer is used for ingesting source data which is AWS S3 in my Project with minimal transformation.

Typical metadata captured during ingestion includes:

- Source record information
- read_timestamp
- file_name
- file_size

The Bronze layer also supports Delta Lake features such as Change Data Feed where configured.
```
Example:-

df.write.format("delta") \
    .option("delta.enableChangeDataFeed", True) \
    .mode("append") \
    .saveAsTable(bronze_table)
```
---

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed data.

Data-processing activities include:

- Data type standardization
- Date conversion
- Invalid-value handling
- String cleansing
- Duplicate handling
- Filtering
- Joining datasets
- Business-rule transformations
- Data validation

For example, invalid or malformed numeric values can be handled before converting them to the required numeric data type.

---

## 🥇 Gold Layer

The Gold layer contains business-ready datasets designed for analytical consumption.

The project creates fact and dimension datasets and an enriched analytical view used by the Databricks dashboard.

Example analytical source:

- fmcg.gold.vw_fact_orders_enriched
- fmcg.gold.dim_customers
- fmcg.gold.dim_gross_price
- fmcg.gold.dim_products
- fmcg.gold.fact_orders
- fmcg.gold.sb_dim_customers
- fmcg.gold.sb_dim_gross_price
- fmcg.gold.sb_dim_products
- fmcg.gold.sb_fact_orders
- fmcg.gold.sb_fact_orders

The Gold layer is used as the primary source for business KPIs and visualizations.

---

## 📐 Dimension Processing

The project contains a dedicated dimension-processing stage.

The project includes dimension datasets such as:

- fmcg.gold.dim_customers
- fmcg.gold.dim_gross_price
- fmcg.gold.dim_products

Dimension processing includes data cleansing, standardization, transformation and preparation of business entities for analytical use. In my Project i used SCD Type - 1 and Type - 2 Techniques to update the dimension tables. 

---

## 📊 Fact Processing

The project contains a dedicated fact-processing stage for transactional/sales data.

Fact processing includes:

- Fact data preparation
- Aggregation
- Data validation
- Full-load processing
- Incremental processing
- Delta Lake merge operations

---

## 🔄 Full Load

The project includes a dedicated notebook for full fact processing:

```
03_fact_data_processing/
└── 01_full_load_fact_retail
```

The full-load process is designed to process the complete available dataset and create/refresh the required fact data.

---

## ⚡ Incremental Load

The project also includes an incremental fact-processing notebook:

```
03_fact_data_processing/
└── 02_incremental_fact_retail
```
Incremental processing is used to avoid unnecessarily reprocessing the complete dataset.

The pipeline uses Delta Lake MERGE operations to:

- Match existing records
- Update matching records
- Insert new records

Example pattern:
```
if not (spark.catalog.tableExists(gold_table)):
    df_gold.write.format("delta")\
        .option("delta.enableChangeDataFeed", True)\
            .mode("overwrite")\
                .option("mergeschema", True)\
                    .saveAsTable(gold_table)
else:
    delta_gold = DeltaTable.forName(spark, gold_table)

    delta_gold.alias("t").merge(
        df_gold.alias("s"),
        "t.order_id == s.order_id and\
        t.date == s.date and\
        t.customer_code == s.customer_code and\
        t.product_code == s.product_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

delta_table_gold = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.fact_{data_source}')

delta_table_gold.alias("t").merge(
    df_child.alias("s"),
    "t.date == s.date_monthly and\
    t.product_code == s.product_code and\
    t.customer_code == s.customer_code"
).whenMatchedUpdate(
    set = {
        "product_code": "s.product_code",
        "customer_code": "s.customer_code",
        "sold_quantity": "s.sold_quantity"
        }
).whenNotMatchedInsert(
    values={
        "date": "s.date_monthly",
        "product_code": "s.product_code",
        "customer_code": "s.customer_code",
        "sold_quantity": "s.sold_quantity"
    }
).execute()
```
---

## 🧱 Delta Lake

Delta Lake is used as the storage layer for reliable data processing.

The project uses Delta tables for operations such as:

- MERGE
- UPDATE
- DELETE
- Incremental processing
- Change Data Feed
- Transactional data management

Delta Lake enables the project to maintain reliable and business-ready datasets throughout the ETL pipeline.

---

## ✅ Data Quality

Data-quality validation is included as part of the processing workflow.

Examples of checks include:

- Null checks
- Duplicate checks
- Data-type validation
- Invalid-value checks
- Business-rule validation
- Source-to-target record-count reconciliation

The objective is to ensure that data moving into the Gold layer is suitable for analytical consumption.

---

## 📊 Dashboard & Analytics

An interactive Databricks SQL Dashboard has been created on top of the Gold-layer analytical view.

Dashboard Data Source

fmcg.gold.vw_fact_orders_enriched

KPI Counters

The dashboard includes:

- Total Revenue
- Total Units Sold
- Total Customers
- Revenue per Customer
- Total Products
- Average Selling Price
- Visualizations

The dashboard includes multiple visualization types:

📈 Monthly Revenue Trend

Shows revenue performance across months.

🥇 Top Customers by Revenue

Identifies products contributing the highest revenue.

🥧 Revenue by Channel

Shows revenue contribution across different sales channels.

🔵 Product Price vs Quantity Analysis

A scatter/bubble-style analysis showing:

Average product price
Total quantity sold
Total revenue
Product division

📋 Customer Revenue Analysis

Provides customer-level quantity and revenue information.

📊 Variant Revenue Analysis

Shows revenue contribution by product variant.

📋 Prodcut Revenue Analysis

Interactive Filters

The dashboard provides filters for:

- Year
- Month
- Market
- Channel
- Division
- Category

These filters allow business users to drill into different aspects of sales and revenue performance.

---

## ⚙️ ETL Job

The complete ETL workflow is orchestrated through the Databricks job:

- job_main_dwh_etl_pipeline

The job coordinates the data-processing workflow across the setup, dimension and fact-processing stages.

## 📁 Project Structure

```
etl-pipeline-retail-fmcg-pyspark-databricks/

├── README.md
│
├── dashboard/
│   └── FMCG Business Insights.pdf
│
├── data-source/
│   ├── full_load_source_file/
│   │   ├── customers/
│   │   ├── gross_price/
│   │   ├── products/
│   │   └── orders/
│   │       └── landing/
│   └── incremental_load_source_file/
│       └── orders/
│
└── notebooks/
    ├── 01_setup/
    │   ├── 1_retail_setup_catalog.py
    │   ├── 2_retail_dim_date_tbl.py
    │   └── 3_utilities.py
    │
    ├── 02_dimension_data_processing/
    │   ├── 1_customer_data_processing_retail.py
    │   ├── 2_products_data_processing_retail.py
    │   └── 3_pricing_data_processing_retail.py
    │
    └── 03_fact_data_processing/
        ├── 1_full_load_fact_retail.py
        └── 2_incremental_fact_retail.py
```
---

## ⭐ Key Features
```
☁️ AWS S3-based source ingestion
📂 Landing and processed file management
⚡ PySpark-based ETL processing
🧱 Bronze/Silver/Gold architecture
📐 Dimension processing
📊 Fact processing
🔄 Full-load pipeline
⚡ Incremental-load pipeline
🔀 Delta Lake MERGE
🧹 Data cleansing and validation
🔍 Duplicate and invalid-record handling
📈 Interactive Databricks SQL Dashboard
📊 Business KPI reporting
🗂️ GitHub-based project documentation
```
---

## 🚀 How to Run
```
1. Clone the repository
git clone https://github.com/DENir1/etl-pipeline-retail-fmcg-pyspark-databricks

2. Upload source files to AWS S3
Place the source FMCG files into the configured S3 landing folder.

s3://child-sportsbar/customers/
s3://child-sportsbar/gross_price/
s3://child-sportsbar/orders/landing/
s3://child-sportsbar/products/

3. Configure Databricks

Configure the required:

Databricks workspace
Catalog
Schemas
S3 access
Source paths

4. Run Setup

Execute the notebooks under:
├── 01_setup/
│   ├── 1_retail_setup_catalog
│   ├── 2_retail_dim_date_tbl
│   └── 3_utilities

5. Run Dimension Processing
Execute the notebooks under:
├── 02_dimension_data_processing/
│   ├── 1_customer_data_processing_retail
│   ├── 2_products_data_processing_retail
│   └── 3_pricing_data_processing_retail

6. Run Fact Processing
For a complete refresh, execute:
├── 03_fact_data_processing/
│   ├── 01_full_load_fact_retail
│   └── 02_incremental_fact_retail

7. Run the Databricks Job
The complete workflow can be orchestrated using:
job_main_dwh_etl_pipeline

8. Open the Dashboard
After successful Gold-layer processing, open the Databricks SQL Dashboard to analyze the business data.
```
---

## 🔮 Future Enhancements
```
- Potential future improvements include:
- Pipeline failure notifications
- Additional ETL monitoring
- More advanced incremental processing
- CI/CD integration for Databricks
- Automated testing
- Additional business KPIs
- Enhanced dashboard drill-downs
```
## Author & Contact

**Neeruj Vijayvargiya**<br>
ETL Developer | Data Engineer
```
Core Skills:-
SQL
T-SQL
PySpark
Databricks
Spark SQL
Delta Lake
AWS S3
ETL
Data Warehousing
Data Engineering

📧 Email: itsniruj@gmail.com
🔗 [LinkedIn](https://www.linkedin.com/in/neeruj-vijayvargiya-2115931ab/)
```
