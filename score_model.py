import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, round as _round

BASE = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("ScoreModel").getOrCreate()

df = spark.read.parquet(f"{BASE}/data/gold/customer_segments")

print("=== Résultats de segmentation clients ===")
summary = (
    df.groupBy("prediction")
    .agg(
        count("CustomerID").alias("NbClients"),
        _round(avg("Monetary"), 2).alias("AvgMonetary"),
        _round(avg("Frequency"), 2).alias("AvgFrequency"),
    )
    .orderBy("prediction")
)
summary.show()

df.write.mode("overwrite").parquet(f"{BASE}/data/gold/customer_segments_scored")
print("✅ Scoring terminé – segments sauvegardés")
spark.stop()
