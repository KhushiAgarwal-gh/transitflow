# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold – Business Aggregates

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG, SCHEMA = "main", "transitflow"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

base = (spark.table(T("silver_trips"))
        .join(spark.table(T("silver_passenger_counts")), "trip_id", "left")
        .join(spark.table(T("silver_routes")), "route_id", "left")
        .withColumn("passengers_boarded", F.coalesce("passengers_boarded", F.lit(0))))

# COMMAND ----------
(base.groupBy("route_id", "route_name", "mode", "trip_date")
 .agg(F.count("*").alias("total_trips"),
      F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_trips"),
      F.sum("passengers_boarded").alias("total_passengers"),
      F.round(F.avg("passengers_boarded"), 2).alias("avg_passengers_per_trip"),
      F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min"),
      F.sum(F.col("is_delayed").cast("int")).alias("delayed_trips"))
 .write.format("delta").mode("overwrite").saveAsTable(T("gold_route_daily")))

(base.filter("status != 'Cancelled'").groupBy("mode", "trip_hour")
 .agg(F.sum("passengers_boarded").alias("total_passengers"),
      F.round(F.avg("passengers_boarded"), 2).alias("avg_passengers_per_trip"),
      F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min"))
 .write.format("delta").mode("overwrite").saveAsTable(T("gold_hourly")))

(base.filter("delay_minutes IS NOT NULL").groupBy("route_id", "route_name", "mode")
 .agg(F.count("*").alias("trips_measured"),
      F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min"),
      F.round(F.expr("percentile_approx(delay_minutes, 0.9)"), 2).alias("p90_delay_min"),
      F.round(100 * F.avg(F.col("is_delayed").cast("int")), 2).alias("pct_delayed"))
 .write.format("delta").mode("overwrite").saveAsTable(T("gold_delay_summary")))

# COMMAND ----------
# MAGIC %md ### Delta Lake time travel demo
# MAGIC `DESCRIBE HISTORY` aur `VERSION AS OF` se purana data dekh sakte ho.

# COMMAND ----------
# MAGIC %sql
# MAGIC DESCRIBE HISTORY main.transitflow.gold_route_daily;
