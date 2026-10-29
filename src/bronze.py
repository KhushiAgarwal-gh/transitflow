"""BRONZE: raw data as-is + ingestion metadata. Koi cleaning nahi."""
from pyspark.sql import functions as F
from . import config
from .spark_utils import write_table


def run(spark):
    for name in ["routes", "trips", "passenger_counts"]:
        df = (spark.read.option("header", True).option("inferSchema", False)
              .csv(str(config.RAW_DIR / f"{name}.csv"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(f"{name}.csv")))
        write_table(df, config.BRONZE / name)
        print(f"[bronze] {name}: {df.count()} rows")
