import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"

# Local lakehouse (Databricks par yahan DBFS / Unity Catalog path aayega)
LAKE_DIR = Path(os.getenv("TRANSITFLOW_LAKE", ROOT / "lakehouse"))
BRONZE = LAKE_DIR / "bronze"
SILVER = LAKE_DIR / "silver"
GOLD = LAKE_DIR / "gold"

# Delta use karna hai ya parquet fallback (local me delta jars na ho to)
TABLE_FORMAT = os.getenv("TRANSITFLOW_FORMAT", "delta")

DELAY_THRESHOLD_MIN = 5   # itne min se zyada late = "delayed" trip
