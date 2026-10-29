"""GOLD: business-level aggregates, analytics/BI ke liye ready."""
from pyspark.sql import functions as F
from . import config
from .spark_utils import read_table, write_table


def run(spark):
    trips = read_table(spark, config.SILVER / "trips")
    pax = read_table(spark, config.SILVER / "passenger_counts")
    routes = read_table(spark, config.SILVER / "routes")

    base = (trips.join(pax, "trip_id", "left")
                 .join(routes, "route_id", "left")
                 .withColumn("passengers_boarded", F.coalesce("passengers_boarded", F.lit(0))))

    # 1) route x day
    route_daily = (base.groupBy("route_id", "route_name", "mode", "trip_date")
        .agg(F.count("*").alias("total_trips"),
             F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_trips"),
             F.sum("passengers_boarded").alias("total_passengers"),
             F.round(F.avg("passengers_boarded"), 2).alias("avg_passengers_per_trip"),
             F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min"),
             F.sum(F.col("is_delayed").cast("int")).alias("delayed_trips")))
    write_table(route_daily, config.GOLD / "route_daily_metrics")

    # 2) hourly ridership
    hourly = (base.filter(F.col("status") != "Cancelled")
        .groupBy("mode", "trip_hour")
        .agg(F.sum("passengers_boarded").alias("total_passengers"),
             F.round(F.avg("passengers_boarded"), 2).alias("avg_passengers_per_trip"),
             F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min")))
    write_table(hourly, config.GOLD / "hourly_ridership")

    # 3) route delay summary (lifetime)
    delay = (base.filter(F.col("delay_minutes").isNotNull())
        .groupBy("route_id", "route_name", "mode")
        .agg(F.count("*").alias("trips_measured"),
             F.round(F.avg("delay_minutes"), 2).alias("avg_delay_min"),
             F.round(F.expr("percentile_approx(delay_minutes, 0.9)"), 2).alias("p90_delay_min"),
             F.round(100 * F.avg(F.col("is_delayed").cast("int")), 2).alias("pct_delayed")))
    write_table(delay, config.GOLD / "route_delay_summary")
    print(f"[gold] route_daily={route_daily.count()} hourly={hourly.count()} delay_summary={delay.count()}")
