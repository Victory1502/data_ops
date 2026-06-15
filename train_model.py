import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

BASE = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.appName("TrainModel").getOrCreate()

df = spark.read.parquet(f"{BASE}/data/gold/customer_features")

df = df.dropna(subset=["Monetary", "Frequency"])

assembler = VectorAssembler(inputCols=["Monetary", "Frequency"], outputCol="features_raw")
df_vec = assembler.transform(df)

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
df_scaled = scaler.fit(df_vec).transform(df_vec)

kmeans = KMeans(featuresCol="features", k=4, seed=42)
model = kmeans.fit(df_scaled)
predictions = model.transform(df_scaled)

evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
score = evaluator.evaluate(predictions)
print(f"✅ Silhouette score: {score:.4f}")

predictions.select("CustomerID", "Monetary", "Frequency", "prediction") \
    .write.mode("overwrite").parquet(f"{BASE}/data/gold/customer_segments")

spark.stop()
