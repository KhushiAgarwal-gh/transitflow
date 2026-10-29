"""Run: python -m src.run_pipeline   (generate -> bronze -> silver -> gold -> SQL reports)"""
import sys
from pathlib import Path
from . import bronze, config, gold, silver
from .generate_data import main as generate
from .spark_utils import get_spark, register_views

SQL_DIR = config.ROOT / "sql"


def run_sql_reports(spark):
    register_views(spark)
    for f in sorted(SQL_DIR.glob("*.sql")):
        if f.name.startswith("00"):
            continue
        print(f"\n===== {f.name} =====")
        spark.sql(f.read_text()).show(15, truncate=False)


def main():
    if not (config.RAW_DIR / "trips.csv").exists() or "--regen" in sys.argv:
        generate()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Table format: {config.TABLE_FORMAT}")
    bronze.run(spark)
    silver.run(spark)
    gold.run(spark)
    run_sql_reports(spark)
    spark.stop()


if __name__ == "__main__":
    main()
