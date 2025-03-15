There are two files in this repository,

BlinkIt Sales Report.pdf and BlinkIt_DataWarehouse.py

The dataset is added in Microsoft Azure Blob Storage and the code to mount it to Databricks is included in the python file

BlinkIt Sales

Overview
This project demonstrates a complete data warehousing solution for BlinkIt sales data, covering ETL processes, dimensional modeling, and analytical querying. The objective is to integrate raw data, transform it into a dimensional model, and execute analytical queries to derive business insights.
Learning Objectives
	•	Apply ETL principles to integrate diverse data sources.
	•	Design and implement dimension tables using proper transformation techniques.
	•	Create fact tables that connect to dimension tables via appropriate keys.
	•	Implement a star or snowflake schema data warehouse.
	•	Develop analytical queries leveraging the dimensional model.
Project Structure
BlinkIt_Sales_Report/
│-- models/                 # SQL scripts for dimensional modeling
│-- python file/                # Analytical queries
│-- README.md               # Project overview and setup instructions
Data Sources
Raw data is stored in Azure Blob Storage and mounted to Databricks at /mnt/blinkit_storage/. The datasets include:
	•	blinkit_customers.csv – Customer profiles and segments
	•	blinkit_orders.csv – Order transactions
	•	blinkit_order_items.csv – Itemized breakdown of orders
	•	blinkit_products.csv – Product catalog with pricing
	•	blinkit_delivery_performance.csv – Delivery time records
	•	blinkit_marketing_performance.csv – Promotional campaign data
	•	blinkit_customer_feedback.csv – Customer ratings and sentiments
	•	blinkit_inventory.csv – Stock movements and damages
ETL Implementation
The ETL pipeline extracts raw data from Azure Blob Storage, transforms it into a dimensional model, and loads it into staging Delta tables in Databricks.
ETL Steps:
	1	Extract: Read raw CSV files from Blob Storage.
	2	Transform:
	◦	Normalize data into staging tables.
	◦	Handle missing values using fillna().
	◦	Implement Slowly Changing Dimensions (SCD Type 2) for Customers and Products.
	3	Load: Store transformed data into fact and dimension tables.
Dimensional Model
The warehouse follows a star schema with:
	•	Fact Table: fact_orders
	◦	Stores sales transactions and revenue metrics.
	•	Dimension Tables:
	◦	dim_customers – Customer information
	◦	dim_products – Product details
	◦	dim_delivery – Delivery partner and performance
	◦	dim_marketing – Marketing campaigns
	◦	dim_feedback – Customer reviews
	◦	dim_time – Time-based analysis
Analytical Queries
Query 1: Top 10 Best Selling Products
SELECT p.product_name, SUM(f.quantity) AS total_units_sold, SUM(f.quantity * f.unit_price) AS total_sales
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 10;
Query 2: On-Time vs. Delayed Delivery
SELECT d.delivery_status, COUNT(f.order_id) AS total_orders, AVG(d.delivery_time_minutes) AS avg_delivery_time
FROM fact_orders f
JOIN dim_delivery d ON f.delivery_partner_id = d.delivery_partner_id
GROUP BY d.delivery_status;
Query 3: Customer Retention Analysis
SELECT COUNT(DISTINCT CASE WHEN c.total_orders > 1 THEN c.customer_id END) AS repeat_customers, COUNT(DISTINCT c.customer_id) AS total_customers
FROM dim_customers c;
Tools & Platforms
	•	Data Warehouse: Databricks (Delta Lake), Azure Blob Storage
	•	ETL Framework: PySpark, Databricks SQL
	•	Querying & Analysis: Spark SQL
	•	Version Control: GitHub
