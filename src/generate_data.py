"""Sample public transport data generate karta hai (routes, trips, passenger counts).
Intentionally thode dirty records daalta hai (duplicates, nulls, negative values)
taaki Silver layer ki cleaning meaningful ho."""
import numpy as np
import pandas as pd
from .config import RAW_DIR

SEED = 42


def make_routes():
    rows = [
        ("R01", "Central - Airport", "Bus", 28.5), ("R02", "North Loop", "Bus", 18.2),
        ("R03", "Harbour Line", "Tram", 12.4), ("R04", "University Express", "Bus", 15.0),
        ("R05", "Green Line", "Metro", 22.8), ("R06", "Blue Line", "Metro", 31.5),
        ("R07", "Old Town Shuttle", "Tram", 6.3), ("R08", "Tech Park Link", "Bus", 20.1),
    ]
    return pd.DataFrame(rows, columns=["route_id", "route_name", "mode", "distance_km"])


# route-wise popularity aur delay-proneness (taaki analytics me fark dikhe)
ROUTE_POP = {"R01": 1.3, "R02": 0.8, "R03": 0.9, "R04": 1.1, "R05": 1.5, "R06": 1.8, "R07": 0.5, "R08": 0.7}
ROUTE_DELAY = {"R01": 0.8, "R02": 1.6, "R03": 1.0, "R04": 0.9, "R05": 0.7, "R06": 1.2, "R07": 1.1, "R08": 1.7}


def make_trips(routes, days=60, per_route_day=14):
    rng = np.random.default_rng(SEED)
    dates = pd.date_range("2026-08-01", periods=days)
    recs, tid = [], 1
    for d in dates:
        for r in routes.route_id:
            for _ in range(per_route_day):
                hour = int(rng.choice(range(5, 23), p=_hour_probs()))
                sched = d + pd.Timedelta(hours=hour, minutes=int(rng.integers(0, 60)))
                rush = hour in (7, 8, 9, 17, 18, 19)
                delay = max(0, rng.normal((6 if rush else 2) * ROUTE_DELAY[r], 4))
                if rng.random() < 0.03:
                    delay += rng.uniform(10, 30)   # incidents
                actual = sched + pd.Timedelta(minutes=float(delay))
                recs.append((f"T{tid:07d}", r, f"V{rng.integers(1, 40):03d}",
                             sched.strftime("%Y-%m-%d %H:%M:%S"),
                             actual.strftime("%Y-%m-%d %H:%M:%S"),
                             rng.choice(["Completed", "Completed", "Completed", "Cancelled"],
                                        p=[0.5, 0.3, 0.17, 0.03])))
                tid += 1
    trips = pd.DataFrame(recs, columns=["trip_id", "route_id", "vehicle_id",
                                        "scheduled_departure", "actual_departure", "status"])
    # dirty data
    dup = trips.sample(150, random_state=1)
    trips = pd.concat([trips, dup], ignore_index=True)
    trips.loc[trips.sample(60, random_state=2).index, "actual_departure"] = None
    trips.loc[trips.sample(40, random_state=3).index, "route_id"] = trips["route_id"].str.lower()
    return trips


def _hour_probs():
    w = np.array([2, 6, 10, 9, 5, 4, 4, 5, 5, 5, 6, 9, 10, 8, 5, 4, 3, 2], dtype=float)
    return w / w.sum()


def make_passengers(trips):
    rng = np.random.default_rng(SEED + 1)
    t = trips.drop_duplicates("trip_id")
    t = t[t.status != "Cancelled"]
    hour = pd.to_datetime(t.scheduled_departure).dt.hour
    base = np.where(hour.isin([7, 8, 9, 17, 18, 19]), 55, 25) * t.route_id.str.upper().map(ROUTE_POP).values
    boarded = np.clip(rng.normal(base, 10), 0, None).astype(int)
    df = pd.DataFrame({"trip_id": t.trip_id.values, "passengers_boarded": boarded,
                       "passengers_alighted": (boarded * rng.uniform(0.85, 1.0, len(t))).astype(int)})
    df.loc[df.sample(40, random_state=4).index, "passengers_boarded"] = -1   # invalid
    df.loc[df.sample(30, random_state=5).index, "passengers_alighted"] = None
    return df


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    routes = make_routes()
    trips = make_trips(routes)
    pax = make_passengers(trips)
    routes.to_csv(RAW_DIR / "routes.csv", index=False)
    trips.to_csv(RAW_DIR / "trips.csv", index=False)
    pax.to_csv(RAW_DIR / "passenger_counts.csv", index=False)
    print(f"routes={len(routes)} trips={len(trips)} passenger_counts={len(pax)} -> {RAW_DIR}")


if __name__ == "__main__":
    main()
