"""SILVER: cleaned, typed, deduplicated, validated data."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from . import config
from .spark_utils import read_table, write_table


def clean_routes(df):
    return (df.select(F.upper(F.trim("route_id")).alias("route_id"),
                      F.trim("route_name").alias("route_name"),
                      F.initcap("mode").alias("mode"),
                      F.col("distance_km").cast("double")).dropDuplicates(["route_id"]))


def clean_trips(df):
    t = (df.withColumn("route_id", F.upper(F.trim("route_id")))
           .withColumn("scheduled_departure", F.to_timestamp("scheduled_departure"))
           .withColumn("actual_departure", F.to_timestamp("actual_departure")))
    # dedupe: same trip_id ka latest ingested record rakho
    w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
    t = t.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    t = (t.withColumn("trip_date", F.to_date("scheduled_departure"))
          .withColumn("trip_hour", F.hour("scheduled_departure"))
          .withColumn("delay_minutes",
                      F.round((F.unix_timestamp("actual_departure")
                               - F.unix_timestamp("scheduled_departure")) / 60, 2))
          .withColumn("is_delayed", F.col("delay_minutes") > config.DELAY_THRESHOLD_MIN))
    return t.select("trip_id", "route_id", "vehicle_id", "scheduled_departure",
                    "actual_departure", "status", "trip_date", "trip_hour",
                    "delay_minutes", "is_delayed")


def clean_passengers(df):
    p = (df.withColumn("passengers_boarded", F.col("passengers_boarded").cast("int"))
           .withColumn("passengers_alighted", F.col("passengers_alighted").cast("int")))
    p = p.filter(F.col("passengers_boarded") >= 0)                       # invalid hatao
    p = p.withColumn("passengers_alighted",
                     F.coalesce("passengers_alighted", F.col("passengers_boarded")))
    return p.dropDuplicates(["trip_id"]).select("trip_id", "passengers_boarded", "passengers_alighted")


def run(spark):
    routes = clean_routes(read_table(spark, config.BRONZE / "routes"))
    trips = clean_trips(read_table(spark, config.BRONZE / "trips"))
    pax = clean_passengers(read_table(spark, config.BRONZE / "passenger_counts"))

    # referential integrity: sirf valid routes wali trips
    valid = trips.join(routes.select("route_id"), "route_id", "inner")
    rejected = trips.join(routes.select("route_id"), "route_id", "left_anti")

    write_table(routes, config.SILVER / "routes")
    write_table(valid, config.SILVER / "trips", partition_by=["trip_date"])
    write_table(pax, config.SILVER / "passenger_counts")
    write_table(rejected, config.SILVER / "_rejected_trips")
    print(f"[silver] routes={routes.count()} trips={valid.count()} "
          f"passenger_counts={pax.count()} rejected_trips={rejected.count()}")
