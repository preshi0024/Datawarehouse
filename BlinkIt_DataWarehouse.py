# Databricks notebook source
# Defining Storage Credentials
storage_account_name = "blinkitwarehouse"
container_name = "blinkit"
storage_account_key = "z+Fvwc4QrpgbHqog+hDXVBb8Oo9pIp7dhhe4CNNswVKVvxy2WJv8OfqU0GA8jDgiUDU159iiMrkx+AStsCvRAg=="

# Mounting Azure Blob Storage
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/blinkit_storage",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# Verifying Mount
display(dbutils.fs.ls("/mnt/blinkit_storage"))

# COMMAND ----------

# Writing raw-data into staging delta tables
df_staging_customerfeedback = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_customer_feedback.csv")

df_staging_customerfeedback.write.format("delta").mode("overwrite").saveAsTable("stg_customerfeedback")

df_staging_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_customers.csv")

df_staging_customers.write.format("delta").mode("overwrite").saveAsTable("stg_customers")

df_staging_deliveryperformance = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_delivery_performance.csv")

df_staging_deliveryperformance.write.format("delta").mode("overwrite").saveAsTable("stg_deliveryperformance")

df_staging_inventory = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_inventory.csv")

df_staging_inventory.write.format("delta").mode("overwrite").saveAsTable("stg_inventory")

df_staging_marketingperformance = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_marketing_performance.csv")

df_staging_marketingperformance.write.format("delta").mode("overwrite").saveAsTable("stg_marketingperformance")

df_staging_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_products.csv")

df_staging_products.write.format("delta").mode("overwrite").saveAsTable("stg_products")

df_staging_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_orders.csv")

df_staging_orders.write.format("delta").mode("overwrite").saveAsTable("stg_orders")

df_staging_orderitems = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/blinkit_storage/blinkit_order_items.csv")

df_staging_orderitems.write.format("delta").mode("overwrite").saveAsTable("stg_orderitems")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, current_date, lit, lead, row_number, year, month, dayofmonth, quarter, weekofyear
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Blinkit_DW").getOrCreate()

# Load Staging Tables
df_stg_customers = spark.read.format("delta").table("stg_customers")
df_stg_products = spark.read.format("delta").table("stg_products")
df_stg_orders = spark.read.format("delta").table("stg_orders")
df_stg_orderitems = spark.read.format("delta").table("stg_orderitems")
df_stg_delivery = spark.read.format("delta").table("stg_deliveryperformance")
df_stg_marketing = spark.read.format("delta").table("stg_marketingperformance")
df_stg_feedback = spark.read.format("delta").table("stg_customerfeedback")
df_stg_inventory = spark.read.format("delta").table("stg_inventory")

# Create Slowly Changing Dimension (SCD Type 2) for Customers
window_spec = Window.partitionBy("customer_id").orderBy(col("registration_date"))
dim_customers = df_stg_customers.withColumn("customer_key", row_number().over(window_spec))
dim_customers = dim_customers.withColumn("start_date", col("registration_date"))
dim_customers = dim_customers.withColumn("end_date", lead("start_date").over(window_spec))
dim_customers = dim_customers.fillna({"end_date": "9999-12-31"})
dim_customers = dim_customers.withColumn("is_current", lit(1))
dim_customers = dim_customers.select(
    "customer_key", "customer_id", "customer_name", "email", "phone", "address", "area", "pincode", "registration_date", "customer_segment", "total_orders", "avg_order_value", "start_date", "end_date", "is_current"
)
dim_customers.write.format("delta").mode("overwrite").saveAsTable("dim_customers")

# Create Slowly Changing Dimension (SCD Type 2) for Products
window_spec_products = Window.partitionBy("product_id").orderBy(col("price"))
dim_products = df_stg_products.withColumn("product_key", row_number().over(window_spec_products))
dim_products = dim_products.withColumn("start_date", current_date())
dim_products = dim_products.withColumn("end_date", lead("start_date").over(window_spec_products))
dim_products = dim_products.fillna({"end_date": "9999-12-31"})
dim_products = dim_products.withColumn("is_current", lit(1))
dim_products = dim_products.select(
    "product_key", "product_id", "product_name", "category", "brand", "price", "mrp", "margin_percentage", "shelf_life_days", "min_stock_level", "max_stock_level", "start_date", "end_date", "is_current"
)
dim_products.write.format("delta").mode("overwrite").saveAsTable("dim_products")

# Delivery Dimension (Normalized without order_id)
dim_delivery = df_stg_delivery.withColumn("delivery_key", monotonically_increasing_id())
dim_delivery = dim_delivery.select(
    "delivery_key", "delivery_partner_id", "promised_time", "actual_time", "delivery_time_minutes", "distance_km", "delivery_status", "reasons_if_delayed"
)
dim_delivery.write.format("delta").mode("overwrite").saveAsTable("dim_delivery")

# Marketing Performance Dimension
dim_marketing = df_stg_marketing.withColumn("marketing_key", monotonically_increasing_id())
dim_marketing = dim_marketing.select(
    "marketing_key", "campaign_id", "campaign_name", "date", "target_audience", "channel", "impressions", "clicks", "conversions", "spend", "revenue_generated", "roas"
)
dim_marketing.write.format("delta").mode("overwrite").saveAsTable("dim_marketing")

# Customer Feedback Dimension
dim_feedback = df_stg_feedback.withColumn("feedback_key", monotonically_increasing_id())
dim_feedback = dim_feedback.select(
    "feedback_key", "feedback_id", "order_id", "customer_id", "rating", "feedback_text", "feedback_category", "sentiment", "feedback_date"
)
dim_feedback.write.format("delta").mode("overwrite").saveAsTable("dim_feedback")

# Inventory Dimension
dim_inventory = df_stg_inventory.withColumn("inventory_key", monotonically_increasing_id())
dim_inventory = dim_inventory.select(
    "inventory_key", "product_id", "date", "stock_received", "damaged_stock"
)
dim_inventory.write.format("delta").mode("overwrite").saveAsTable("dim_inventory")

# Expanded Time Dimension
dim_time = df_stg_orders.select("order_date").distinct()
dim_time = dim_time.withColumn("time_key", monotonically_increasing_id())
dim_time = dim_time.withColumn("year", year("order_date"))
dim_time = dim_time.withColumn("month", month("order_date"))
dim_time = dim_time.withColumn("day", dayofmonth("order_date"))
dim_time = dim_time.withColumn("quarter", quarter("order_date"))
dim_time = dim_time.withColumn("week", weekofyear("order_date"))
dim_time.write.format("delta").mode("overwrite").saveAsTable("dim_time")

# Optimized Fact Table with Partitioning
fact_orders = df_stg_orderitems.alias("oi").join(
    df_stg_orders.alias("o"), col("oi.order_id") == col("o.order_id"), "inner").select(
    col("oi.order_id"), col("oi.product_id"), col("o.customer_id"), col("oi.quantity"), col("oi.unit_price"), col("o.order_total"), col("o.payment_method"), col("o.delivery_partner_id"), col("o.store_id"), col("o.order_date").alias("time_key")
)
fact_orders.write.format("delta").partitionBy("time_key").mode("overwrite").saveAsTable("fact_orders")


# COMMAND ----------

# ------------- Analytical Queries ------------

df_top_products = spark.sql("""
    SELECT 
        p.product_name,
        SUM(f.quantity) AS total_units_sold,
        SUM(f.quantity * f.unit_price) AS total_sales
    FROM fact_orders f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_name
    ORDER BY total_sales DESC
    LIMIT 10
""")

# Convert to Pandas for visualization
df_top_products_pd = df_top_products.toPandas()

# Visualization: Horizontal Bar Chart
plt.figure(figsize=(10,5))
plt.barh(df_top_products_pd["product_name"], df_top_products_pd["total_sales"], color="orange")
plt.xlabel("Total Sales ($)")
plt.ylabel("Product Name")
plt.title("Top 10 Best-Selling Products")
plt.gca().invert_yaxis()  # Invert y-axis for better readability
plt.show()


# COMMAND ----------

df_delivery_performance = spark.sql("""
    SELECT 
        d.delivery_status,
        COUNT(f.order_id) AS total_orders,
        AVG(d.delivery_time_minutes) AS avg_delivery_time
    FROM fact_orders f
    JOIN dim_delivery d ON f.delivery_partner_id = d.delivery_partner_id
    GROUP BY d.delivery_status
""")

# Convert to Pandas for visualization
df_delivery_performance_pd = df_delivery_performance.toPandas()

# Visualization: Pie Chart
plt.figure(figsize=(6,6))
plt.pie(df_delivery_performance_pd["total_orders"], labels=df_delivery_performance_pd["delivery_status"], autopct="%1.1f%%", colors=["lightgreen", "red", "orange"])
plt.title("Delivery Performance: On-Time vs Delayed")
plt.show()


# COMMAND ----------

df_repeat_customers = spark.sql("""
    SELECT 
        COUNT(DISTINCT CASE WHEN c.total_orders > 1 THEN c.customer_id END) AS repeat_customers,
        COUNT(DISTINCT c.customer_id) AS total_customers
    FROM dim_customers c
""")

# Convert to Pandas for visualization
df_repeat_customers_pd = df_repeat_customers.toPandas()

# Compute percentage
repeat_percentage = (df_repeat_customers_pd["repeat_customers"][0] / df_repeat_customers_pd["total_customers"][0]) * 100
one_time_percentage = 100 - repeat_percentage

# Visualization: Pie Chart
plt.figure(figsize=(6,6))
plt.pie([repeat_percentage, one_time_percentage], labels=["Repeat Customers", "One-time Customers"], autopct="%1.1f%%", colors=["blue", "gray"])
plt.title("Customer Retention: Repeat Orders Analysis")
plt.show()


# COMMAND ----------

df_delivery_time = spark.sql("""
    SELECT 
        d.delivery_partner_id,
        AVG(d.delivery_time_minutes) AS avg_delivery_time,
        COUNT(f.order_id) AS total_deliveries
    FROM fact_orders f
    JOIN dim_delivery d ON f.delivery_partner_id = d.delivery_partner_id
    GROUP BY d.delivery_partner_id
    ORDER BY avg_delivery_time
""")

# Convert to Pandas
df_delivery_time_pd = df_delivery_time.toPandas()

# Visualization: Bar Chart of Delivery Partner Performance
plt.figure(figsize=(12,6))
plt.bar(df_delivery_time_pd["delivery_partner_id"], df_delivery_time_pd["avg_delivery_time"], color="purple")
plt.xlabel("Delivery Partner ID")
plt.ylabel("Avg Delivery Time (Minutes)")
plt.title("Average Delivery Time by Partner")
plt.xticks(rotation=45)
plt.show()
