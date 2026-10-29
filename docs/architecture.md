# TransitFlow Architecture (Medallion)

```
 CSV / API sources                 BRONZE                SILVER                  GOLD                 CONSUMERS
┌──────────────────┐        ┌────────────────┐    ┌──────────────────┐    ┌─────────────────────┐   ┌─────────────┐
│ trips.csv        │        │ bronze_trips   │    │ silver_trips     │    │ gold_route_daily    │   │ SQL queries │
│ passenger_counts │ ─────► │ bronze_pax     │ ─► │ silver_pax       │ ─► │ gold_hourly         │ ─►│ Dashboards  │
│ routes.csv       │ ingest │ bronze_routes  │    │ silver_routes    │    │ gold_delay_summary  │   │ BI / Alerts │
└──────────────────┘        └────────────────┘    └──────────────────┘    └─────────────────────┘   └─────────────┘
                             raw + metadata        clean, typed, deduped    business aggregates
                             (_ingested_at)        validated, partitioned
```

| Layer  | Purpose | Key transformations |
|--------|---------|---------------------|
| Bronze | Raw, immutable copy | Add `_ingested_at`, `_source_file`; no cleaning |
| Silver | Trusted data | Type casting, dedupe (window fn), route-id normalisation, drop negative counts, fill missing alighted, delay calculation, FK check against routes |
| Gold   | Analytics-ready | Route x day metrics, hourly ridership, route delay summary (avg, p90, % delayed) |

## Design decisions
- **Delta Lake** for ACID writes, time travel, `OPTIMIZE`/`ZORDER`, CHECK constraints.
- Silver trips **partitioned by `trip_date`** (daily-range queries fast).
- Rejected rows (unknown route) are kept in `_rejected_trips` instead of silently dropped.
- `TABLE_FORMAT` env var lets the same code run on parquet locally if Delta jars are unavailable.
