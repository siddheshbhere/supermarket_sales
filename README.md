# Supermarket Sales Data Pipeline

This document outlines the steps to extract, transform, and analyze supermarket sales data using Python. The process includes downloading the dataset from Kaggle, transforming it into a structured format suitable for analysis, and exporting summarized insights.

## 1. Extract Supermarket Sales Dataset Using Kaggle API

The Kaggle API is used to download the dataset directly from Kaggle. This ensures easy access to the data and eliminates manual download steps.

```python
from kaggle.api.kaggle_api_extended import KaggleApi

# Initialize the Kaggle API
api = KaggleApi()

# Authenticate using the kaggle.json file
api.authenticate()

# Download the dataset
dataset_name = 'aungpyaeap/supermarket-sales'  # Replace with the dataset identifier
download_path = r'F:\supermarket_sales'  # Specify the folder where you want to download the dataset

api.dataset_download_files(dataset_name, path=download_path, unzip=True)
```

## 2. Data Transformation Script

This step involves cleaning and transforming the raw data into structured formats. The transformed data is stored in a SQLite database with separate dimensions for branches, customers, and products, and a fact table for sales.

### Steps:

1. **Load the raw CSV data**: The raw data is read into a Pandas DataFrame.
2. **Rename columns**: Adjust column names to be SQLite-compliant by converting them to lowercase and replacing spaces with underscores.
3. **Date formatting**: Convert the date column into a standard `YYYY-MM-DD` format.
4. **Create dimensions**: Separate the data into branch, customer, and product dimensions, each with unique identifiers.
5. **Merge dimensions**: Map dimension IDs back into the main sales dataset.
6. **Store in SQLite**: Insert transformed data into respective SQLite tables.

```python
import pandas as pd
import sqlite3

# Load CSV file
df = pd.read_csv('F:/supermarket_sales/supermarket_sales - Sheet1.csv')

# Rename columns to match SQLite naming conventions (lowercase and replace spaces with underscores)
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Convert 'date' column from MM/DD/YYYY to YYYY-MM-DD
df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

# Establish SQLite connection
conn = sqlite3.connect('sales.db')
cursor = conn.cursor()

# 1. Transforming the Branch Data (unique branch, city combinations)
branch_df = df[['branch', 'city']].drop_duplicates()
branch_df['branch_id'] = range(1, len(branch_df) + 1)  # Automatically creating the branch_id
branch_df = branch_df[['branch_id', 'branch', 'city']]

# 2. Transforming the Customer Data (unique customer_type, gender combinations)
customer_df = df[['customer_type', 'gender']].drop_duplicates()
customer_df['customer_id'] = range(1, len(customer_df) + 1)  # Automatically creating the customer_id
customer_df = customer_df[['customer_id', 'customer_type', 'gender']]

# 3. Transforming the Product Data (unique product_line, unit_price, cogs combinations)
product_df = df[['product_line', 'unit_price', 'cogs']].drop_duplicates()
product_df['product_id'] = range(1, len(product_df) + 1)  # Automatically creating the product_id
product_df = product_df[['product_id', 'product_line', 'unit_price', 'cogs']]

# 4. Insert data into the Branch Dimension
branch_df.to_sql('branch', conn, if_exists='replace', index=False)

# 5. Insert data into the Customer Dimension
customer_df.to_sql('customer', conn, if_exists='replace', index=False)

# 6. Insert data into the Product Dimension
product_df.to_sql('product', conn, if_exists='replace', index=False)

# 7. Mapping customer, product, and branch ids from the dimensions into the sales data
df = df.merge(branch_df, on=['branch','city'], how='left')
df = df.merge(customer_df, on=['customer_type', 'gender'], how='left')
df = df.merge(product_df, on=['product_line', 'unit_price', 'cogs'], how='left')

# 8. Selecting required columns for the Sales Fact Table
sales_data = df[['invoice_id', 'branch_id', 'customer_id', 'product_id', 'quantity', 'total', 'gross_income', 'date', 'time', 'payment', 'rating']]

# 9. Insert data into the Sales Fact Table
sales_data.to_sql('sales', conn, if_exists='replace', index=False)

# Commit changes and close connection
conn.commit()
conn.close()
```

## 3. Export SQL Query Data as CSV File

The final step involves generating a report based on a SQL query. The query ranks product lines by sales within each branch and exports the top 3 product lines per branch to a CSV file.

### Steps:

1. **Connect to SQLite**: Establish a connection to the SQLite database.
2. **Execute SQL query**: Run a query to compute total sales per product line and rank them within each branch.
3. **Export to CSV**: Save the resulting data into a CSV file for further analysis.

```python
import pandas as pd
import sqlite3

# Establish SQLite connection
conn = sqlite3.connect('sales.db')
cursor = conn.cursor()

# Load SQL query data into data frame
export_df = pd.read_sql("""WITH CTE AS(
SELECT
    b.branch,
    p.product_line,
    ROUND(SUM(s.total), 2) AS total_sales,
    RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(SUM(s.total), 2) DESC) AS r
FROM
    sales s
JOIN
    branch b ON s.branch_id = b.branch_id
JOIN
    product p ON s.product_id = p.product_id
GROUP BY
    b.branch, p.product_line
)
SELECT branch,
       product_line,
       total_sales
FROM CTE
WHERE r <= 3;
""", conn)

# Export data frame data to CSV file
export_df.to_csv("report.csv", index=False)

# Commit changes and close connection
conn.commit()
conn.close()
```

## 4. Reporting Queries

The following SQL queries provide further insights into sales data:

### Top 3 Product Lines Based on Total Sales in Each Branch

This query identifies the top three product lines based on total sales for each branch, providing insights into the most popular product categories.

```sql
WITH CTE AS(
SELECT
    b.branch,
    p.product_line,
    ROUND(SUM(s.total), 2) AS total_sales,
    RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(SUM(s.total), 2) DESC) AS r
FROM
    sales s
JOIN
    branch b ON s.branch_id = b.branch_id
JOIN
    product p ON s.product_id = p.product_id
GROUP BY
    b.branch, p.product_line
)
SELECT branch,
       product_line,
       total_sales
FROM CTE
WHERE r <= 3;
```

### Top 3 Product Lines Based on Average Customer Rating in Each Branch

This query determines the top three product lines based on average customer ratings for each branch, highlighting customer preferences for quality or satisfaction.

```sql
WITH CTE AS(
SELECT
    b.branch,
    p.product_line,
    ROUND(AVG(s.rating), 2) AS avg_rating,
    RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(AVG(s.rating), 2) DESC) AS r
FROM
    sales s
JOIN
    branch b ON s.branch_id = b.branch_id
JOIN
    product p ON s.product_id = p.product_id
GROUP BY
    b.branch, p.product_line
)
SELECT branch,
       product_line,
       avg_rating
FROM CTE
WHERE r <= 3;
```

### Monthly Sales by Branch

This query aggregates total sales by branch on a monthly basis, helping track trends over time for each branch.

```sql
SELECT
    b.branch,
    strftime('%Y-%m', s.date) AS sales_month,
    ROUND(SUM(s.total), 2) AS total_sales
FROM
    sales s
JOIN
    branch b ON s.branch_id = b.branch_id
GROUP BY
    b.branch, sales_month
ORDER BY
    b.branch, sales_month;
```

### Sales Distribution by Payment Method

This query analyzes the distribution of sales by payment methods, offering insights into customer payment preferences.

```sql
SELECT
    s.payment,
    COUNT(*) AS num_transactions,
    ROUND(SUM(s.total), 2) AS total_sales
FROM
    sales s
GROUP BY
    s.payment
ORDER BY
    total_sales DESC;
```

## 5. Cloud Architecture Diagram

![Project Workflow Image Placeholder](supermarket_sales.png)

### Data Extraction

A Python script utilizes the Kaggle API to extract the supermarket sales dataset, unzips it, and stores the data as a CSV file in Google Cloud Storage.

### Data Transformation

- The Python script reads the source CSV file into a Pandas DataFrame.
- Performs required transformations, such as:
  - Renaming columns for consistency.
  - Converting date formats.
  - Normalizing data into dimensions (e.g., Branch, Customer, Product).
- The transformed data is stored in cloud-based databases such as BigQuery or Cloud SQL.

### Cloud Scheduler

Automate the execution of the data extraction and transformation scripts using Cloud Scheduler for periodic updates. (If required)</br>
**Note:** For this solution we can utilize Cloud Storage trigger for cloud function as well

### Data Analytics

- Use SQL queries to analyze the transformed data and create views or tables for reporting.
- Example:
  - Aggregated sales by branch and product line.
  - Top-performing product lines by sales or customer ratings.
  - Monthly sales trends.

### Business Intelligence & Reporting

- Use the transformed and analyzed data as the source for visualizations and insights.
- Create interactive dashboards with required KPIs using a BI tool such as Looker Studio or Tableau.
