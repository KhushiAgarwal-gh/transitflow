# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver – Cleaning & Validation
# MAGIC Type casting, dedup, null handling, delay calculation, referential integrity.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG, SCHEMA = "main", "transitflow"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
DELAY_THRESHOLD_MIN = 5

# COMMAND ----------
routes = (spark.table(T("bronze_routes"))
          .select(F.upper(F.trim("route_id")).alias("route_id"), "route_name",
                  F.initcap("mode").alias("mode"), F.col("distance_km").cast("double"))
          .dropDuplicates(["route_id"]))

w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
trips = (spark.table(T("bronze_trips"))
         .withColumn("route_id", F.upper(F.trim("route_id")))
         .withColumn("scheduled_departure", F.to_timestamp("scheduled_departure"))
         .withColumn("actual_departure", F.to_timestamp("actual_departure"))
         .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
         .withColumn("trip_date", F.to_date("scheduled_departure"))
         .withColumn("trip_hour", F.hour("scheduled_departure"))
         .withColumn("delay_minutes", F.round((F.unix_timestamp("actual_departure")
                     - F.unix_timestamp("scheduled_departure")) / 60, 2))
         .withColumn("is_delayed", F.col("delay_minutes") > DELAY_THRESHOLD_MIN)
         .join(routes.select("route_id"), "route_id", "inner"))

pax = (spark.table(T("bronze_passenger_counts"))
       .withColumn("passengers_boarded", F.col("passengers_boarded").cast("int"))
       .withColumn("passengers_alighted", F.col("passengers_alighted").cast("int"))
       .filter("passengers_boarded >= 0")
       .withColumn("passengers_alighted", F.coalesce("passengers_alighted", "passengers_boarded"))
       .dropDuplicates(["trip_id"]))

# COMMAND ----------
routes.write.format("delta").mode("overwrite").saveAsTable(T("silver_routes"))
trips.write.format("delta").mode("overwrite").partitionBy("trip_date").saveAsTable(T("silver_trips"))
pax.write.format("delta").mode("overwrite").saveAsTable(T("silver_passenger_counts"))

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Delta: performance + data quality
# MAGIC OPTIMIZE main.transitflow.silver_trips ZORDER BY (route_id);
# MAGIC ALTER TABLE main.transitflow.silver_trips ADD CONSTRAINT valid_hour CHECK (trip_hour BETWEEN 0 AND 23);
