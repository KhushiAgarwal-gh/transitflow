# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze – Raw Ingestion
# MAGIC Raw CSVs ko Delta tables me as-is load karta hai + ingestion metadata.

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "transitflow"          # apne workspace ke hisaab se badlo
RAW_PATH = "/Volumes/main/transitflow/raw"       # CSVs yahan upload karo

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------
for name in ["routes", "trips", "passenger_counts"]:
    df = (spark.read.option("header", True).csv(f"{RAW_PATH}/{name}.csv")
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(f"{name}.csv")))
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{name}"))
    print(name, df.count())
