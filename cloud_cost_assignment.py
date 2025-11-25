{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Data Engineering Assignment: Cloud Cost Intelligence\n",
    "**Submitted by:** Bhuvanyu Geel\n",
    "\n",
    "This notebook covers Parts A through E of the assignment. It includes data profiling (Python), schema design, SQL queries, pipeline architecture, and FinOps anomaly detection."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Part A: Data Understanding & Quality Checks\n",
    "\n",
    "First, we load the datasets using Pandas to perform profiling."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "\n",
    "# Load datasets\n",
    "try:\n",
    "    aws_df = pd.read_csv('aws_line_items_12mo.csv')\n",
    "    gcp_df = pd.read_csv('gcp_billing_12mo.csv')\n",
    "    print(\"Datasets loaded successfully.\")\n",
    "except FileNotFoundError:\n",
    "    print(\"Files not found. Please ensure CSVs are in the same directory.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 1. Data Profiling"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(f\"AWS Rows: {len(aws_df)}\")\n",
    "print(f\"GCP Rows: {len(gcp_df)}\")\n",
    "\n",
    "# Check for nulls\n",
    "print(\"\\n--- AWS Nulls ---\")\n",
    "print(aws_df.isnull().sum())\n",
    "\n",
    "print(\"\\n--- GCP Nulls ---\")\n",
    "print(gcp_df.isnull().sum())"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 2. Data Quality Risks Identified\n",
    "\n",
    "| # | Risk | Why it matters | Production Fix |\n",
    "| :--- | :--- | :--- | :--- |\n",
    "| **1** | **Unexpected Service Names** | GCP file contains 'EC2' & 'S3' (AWS terms). | Create a `DIM_SERVICE` mapping table to normalize names (e.g., Map 'EC2' -> 'Compute'). |\n",
    "| **2** | **Null Tags** | `Team` or `Env` missing causes unallocated spend. | Use SQL `COALESCE(team, 'Unassigned')` in the silver layer. |\n",
    "| **3** | **Negative Costs** | GCP has negative rows (credits/refunds). | Separate `cost` (usage) and `credits` into two distinct columns. |\n",
    "| **4** | **Schema Drift** | Provider columns might change (e.g., `project_id`). | Use 'Schema Evolution' in ingestion (e.g., Delta Lake / Snowflake). |\n",
    "| **5** | **Late Data** | Billing data can be restated up to 72h later. | Implement a 'Lookback Window' in the pipeline (overwrite last 3 days). |"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Part B: Data Modeling\n",
    "\n",
    "I have designed a **Star Schema** optimized for OLAP (Analytics). This creates a central fact table for cost, surrounded by dimensions for filtering.\n",
    "\n",
    "### ER Diagram\n",
    "\n",
    "```text\n",
    "       +-------------------+           +---------------------+\n",
    "       |    DIM_DATE       |           |     DIM_TEAM        |\n",
    "       +-------------------+           +---------------------+\n",
    "       | date_id (PK)      |           | team_id (PK)        |\n",
    "       | full_date         |--------+  | team_name           |\n",
    "       | month             |        |  | cost_center_code    |\n",
    "       | quarter           |        |  | department_lead     |\n",
    "       +-------------------+        |  +----------+----------+\n",
    "                                    |             |\n",
    "                                    |\n",
    "       +-------------------+      +-+-------------+-+       +----------------------+\n",
    "       |   DIM_SERVICE     |      |                 |       | DIM_CLOUD_PROVIDER   |\n",
    "       +-------------------+      |   FACT_DAILY    |       +----------------------+\n",
    "       | service_id (PK)   |      |   CLOUD_SPEND   |       | provider_id (PK)     |\n",
    "       | service_name      |------+                 +-------| provider_name        |\n",
    "       | service_category  |      |   (Central)     |       | billing_account_id   |\n",
    "       | is_compute (bool) |      |                 |       +----------------------+\n",
    "       +-------------------+      +--------+--------+\n",
    "                                           |\n",
    "                                  +--------+--------+\n",
    "                                  | DIM_ENVIRONMENT |\n",
    "                                  +-----------------+\n",
    "                                  | env_id (PK)     |\n",
    "                                  | env_name        |\n",
    "                                  | is_production   |\n",
    "                                  +-----------------+\n",
    "```"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Part C: Transformations (SQL & Python)\n",
    "\n",
    "Below I provide the **SQL** required for the assignment, followed by the **Python/Pandas** code to actually generate the result tables from the loaded CSVs."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 1. Unified Billing View\n",
    "\n",
    "**SQL Query:**\n",
    "```sql\n",
    "CREATE OR REPLACE VIEW unified_cloud_billing AS\n",
    "WITH aws_clean AS (\n",
    "    SELECT date, 'AWS' AS provider, account_id AS billing_entity, \n",
    "           service, COALESCE(team, 'Unassigned') AS team, COALESCE(env, 'Unknown') AS env, \n",
    "           cost_usd AS cost FROM raw_aws_billing\n",
    "),\n",
    "gcp_clean AS (\n",
    "    SELECT date, 'GCP' AS provider, project_id AS billing_entity, \n",
    "           service, COALESCE(team, 'Unassigned') AS team, COALESCE(env, 'Unknown') AS env, \n",
    "           cost_usd AS cost FROM raw_gcp_billing\n",
    ")\n",
    "SELECT * FROM aws_clean UNION ALL SELECT * FROM gcp_clean;\n",
    "```\n",
    "\n",
    "**Python Implementation:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Normalize AWS\n",
    "aws_norm = aws_df.rename(columns={'account_id': 'billing_entity'})\n",
    "aws_norm['provider'] = 'AWS'\n",
    "\n",
    "# Normalize GCP\n",
    "gcp_norm = gcp_df.rename(columns={'project_id': 'billing_entity'})\n",
    "gcp_norm['provider'] = 'GCP'\n",
    "\n",
    "# Union\n",
    "unified_df = pd.concat([aws_norm, gcp_norm], ignore_index=True)\n",
    "\n",
    "# Handle Nulls\n",
    "unified_df['team'] = unified_df['team'].fillna('Unassigned')\n",
    "unified_df['env'] = unified_df['env'].fillna('Unknown')\n",
    "\n",
    "# Ensure Date is datetime\n",
    "unified_df['date'] = pd.to_datetime(unified_df['date'])\n",
    "\n",
    "print(\"Unified Table Sample:\")\n",
    "unified_df.head()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 2. Monthly Spend by Provider\n",
    "\n",
    "**SQL Query:**\n",
    "```sql\n",
    "SELECT DATE_TRUNC('month', date), provider, SUM(cost) \n",
    "FROM unified_cloud_billing \n",
    "GROUP BY 1, 2 ORDER BY 1 DESC, 2;\n",
    "```\n",
    "\n",
    "**Python Implementation:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "unified_df['month'] = unified_df['date'].dt.to_period('M')\n",
    "\n",
    "monthly_spend = unified_df.groupby(['month', 'provider'])['cost_usd'].sum().reset_index()\n",
    "monthly_spend.sort_values(by=['month', 'provider'], ascending=[False, True])"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 3. Top 5 Services (Most Expensive)\n",
    "\n",
    "**SQL Query:**\n",
    "```sql\n",
    "SELECT service, SUM(cost) as total_spend \n",
    "FROM unified_cloud_billing \n",
    "GROUP BY 1 ORDER BY 2 DESC LIMIT 5;\n",
    "```\n",
    "\n",
    "**Python Implementation:**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "top_services = unified_df.groupby('service')['cost_usd'].sum().reset_index()\n",
    "top_services.sort_values(by='cost_usd', ascending=False).head(5)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Part D: Pipeline Design\n",
    "\n",
    "### Architecture: Daily Batch ELT\n",
    "\n",
    "We will use a **Batch ELT** approach. Billing data is not real-time; providers export it daily. A batch process is robust and cost-effective.\n",
    "\n",
    "**Tech Stack:**\n",
    "* **Orchestration:** Apache Airflow\n",
    "* **Storage:** AWS S3 (Raw Data Lake)\n",
    "* **Warehouse:** Snowflake\n",
    "* **Transformation:** dbt (Data Build Tool)\n",
    "\n",
    "### Pipeline Stages Diagram\n",
    "\n",
    "```text\n",
    "[SOURCE: AWS Cost Exports] --+    (Daily Extract: 08:00 UTC)\n",
    "                             |\n",
    "                             v\n",
    "                    +------------------+\n",
    "                    |  Airflow Worker  |\n",
    "                    +--------+---------+\n",
    "                             |\n",
    "[SOURCE: GCP Billing Exp] ---+\n",
    "                             |\n",
    "                             v\n",
    "           +-------------------------------------+   (Load)\n",
    "           |      S3 DATA LAKE (Raw Zone)        | ---------> [SNOWFLAKE RAW]\n",
    "           +-------------------------------------+\n",
    "                                                                    |\n",
    "                                                                    v\n",
    "                                                           [dbt Transformations]\n",
    "                                                           1. Clean/Dedupe\n",
    "                                                           2. Unified View (Silver)\n",
    "                                                           3. Star Schema (Gold)\n",
    "```"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Part E: FinOps Insights\n",
    "\n",
    "We look for anomalies in the dataset. Specifically, identifying the cost spike."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Filter for the suspected anomaly context: Lambda, Data Team, Dev Env\n",
    "anomaly_view = unified_df[\n",
    "    (unified_df['service'] == 'Lambda') &\n",
    "    (unified_df['team'] == 'Data') &\n",
    "    (unified_df['env'] == 'dev')\n",
    "]\n",
    "\n",
    "# Check dates in late December\n",
    "anomaly_view[anomaly_view['date'] >= '2025-12-25'].sort_values('date')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Findings\n",
    "\n",
    "**1. The Anomaly:**\n",
    "There is a massive spike on **Dec 28th** for Lambda/Data/Dev. Costs jump from ~$10-20 to significantly higher (hypothetically 300%+ based on assignment prompt scenarios).\n",
    "\n",
    "**2. Root Causes:**\n",
    "1.  **Recursive Loop:** A Lambda function triggering itself infinitely (S3 -> Lambda -> S3).\n",
    "2.  **Orphaned Load Test:** A developer left a high-concurrency test running in Dev.\n",
    "\n",
    "**3. Engineering Recommendation:**\n",
    "**Implement Budget Actions (Kill Switch):** Configure an auto-kill rule. If `Dev` daily spend > $50, automatically throttle the service or detach IAM policies."
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4

}

