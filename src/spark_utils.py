"""SparkSession + table read/write helpers.
Databricks par spark pehle se milta hai aur Delta built-in hota hai.
Local par agar delta-spark jars mil jayein to Delta, nahi to parquet fallback."""
from pyspark.sql import DataFrame, SparkSession
from . import config


def get_spark(app="TransitFlow") -> SparkSession:
    builder = (SparkSession.builder.appName(app)
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false"))
    if config.TABLE_FORMAT == "delta":
        try:
            from delta import configure_spark_with_delta_pip
            builder = (builder
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog",
                               "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
            return configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception as e:   # delta-spark install nahi / jars unavailable
            print(f"[warn] Delta unavailable ({type(e).__name__}) -> parquet fallback")
            config.TABLE_FORMAT = "parquet"
    return builder.getOrCreate()


def write_table(df: DataFrame, path, mode="overwrite", partition_by=None):
    w = df.write.format(config.TABLE_FORMAT).mode(mode)
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(str(path))


def read_table(spark: SparkSession, path) -> DataFrame:
    return spark.read.format(config.TABLE_FORMAT).load(str(path))


def register_views(spark: SparkSession):
    """Gold/Silver tables ko SQL views ki tarah register karta hai (sql/ queries ke liye)."""
    names = {
        "silver_trips": config.SILVER / "trips",
        "silver_passenger_counts": config.SILVER / "passenger_counts",
        "silver_routes": config.SILVER / "routes",
        "gold_route_daily": config.GOLD / "route_daily_metrics",
        "gold_hourly": config.GOLD / "hourly_ridership",
        "gold_delay_summary": config.GOLD / "route_delay_summary",
    }
    for n, p in names.items():
        read_table(spark, p).createOrReplaceTempView(n)
