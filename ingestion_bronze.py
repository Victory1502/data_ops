import os
from pyspark.sql import SparkSession

BASE = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("IngestBronze").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/data.csv")

df.write.mode("overwrite").option("header", "true").csv(f"{BASE}/data/bronze/retail_raw")

print(f"✅ Ingestion terminée – {df.count()} lignes sauvegardées dans data/bronze/retail_raw")
spark.stop()
