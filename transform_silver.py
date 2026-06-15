import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper

BASE = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("TransformSilver").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/data/bronze/retail_raw")

df_clean = (
    df
    .dropDuplicates()
    .dropna(subset=["InvoiceNo", "StockCode", "CustomerID", "InvoiceDate"])
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
    .withColumn("Description", trim(upper(col("Description"))))
    .filter(col("Quantity") > 0)
    .filter(col("UnitPrice") > 0)
    .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
)

df_clean.write.mode("overwrite").parquet(f"{BASE}/data/silver/retail_clean")

print(f"✅ Silver terminé – {df_clean.count()} lignes nettoyées")
spark.stop()
