import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, to_date

BASE = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("AggregateGold").getOrCreate()

df = spark.read.parquet(f"{BASE}/data/silver/retail_clean")

df = df.withColumn("InvoiceDateOnly", to_date(col("InvoiceDate")))

sales_daily = (
    df.groupBy("InvoiceDateOnly", "StockCode", "Description")
    .agg(
        _sum("Quantity").alias("TotalQuantity"),
        _sum("TotalPrice").alias("TotalRevenue")
    )
)

sales_daily.write.mode("overwrite") \
    .partitionBy("InvoiceDateOnly") \
    .parquet(f"{BASE}/data/gold/sales_daily")

customer_features = (
    df.groupBy("CustomerID")
    .agg(
        _sum("TotalPrice").alias("Monetary"),
        countDistinct("InvoiceNo").alias("Frequency")
    )
)

customer_features.write.mode("overwrite").parquet(f"{BASE}/data/gold/customer_features")

print("✅ Gold terminé")
spark.stop()
