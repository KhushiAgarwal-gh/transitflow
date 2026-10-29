import pytest
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from src.silver import clean_passengers, clean_trips


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_trips_dedup_and_delay(spark):
    rows = [("T1", "r01", "V1", "2026-08-01 08:00:00", "2026-08-01 08:10:00", "Completed", "2026-01-01 00:00:00"),
            ("T1", "R01", "V1", "2026-08-01 08:00:00", "2026-08-01 08:10:00", "Completed", "2026-01-02 00:00:00")]
    df = spark.createDataFrame(rows, ["trip_id", "route_id", "vehicle_id", "scheduled_departure",
                                      "actual_departure", "status", "_ingested_at"]
                               ).withColumn("_ingested_at", __import__("pyspark.sql.functions", fromlist=["x"]).to_timestamp("_ingested_at"))
    out = clean_trips(df).collect()
    assert len(out) == 1
    assert out[0].route_id == "R01"
    assert out[0].delay_minutes == 10.0 and out[0].is_delayed is True


def test_passengers_remove_negative(spark):
    df = spark.createDataFrame([("T1", "-1", "5"), ("T2", "30", None)],
                               ["trip_id", "passengers_boarded", "passengers_alighted"])
    out = {r.trip_id: r for r in clean_passengers(df).collect()}
    assert "T1" not in out
    assert out["T2"].passengers_alighted == 30
