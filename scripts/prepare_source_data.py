"""Download Jan-2023 NYC taxi data, shift dates to the last 31 days
(BigQuery Sandbox expires partitions 60 days after their date), write Parquet."""
import pathlib
import urllib.request
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OUT = pathlib.Path("data")

SCHEMA = pa.schema([
    ("vendorid", pa.int64()), ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")), ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()), ("ratecodeid", pa.int64()),
    ("store_and_fwd_flag", pa.string()), ("pulocationid", pa.int64()),
    ("dolocationid", pa.int64()), ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()), ("extra", pa.float64()), ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()), ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()), ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()), ("airport_fee", pa.float64()),
])


def download(url: str, dest: pathlib.Path) -> pathlib.Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, dest)
    return dest


def shift(col, delta_us: int):
    as_int = pc.cast(col, pa.int64())
    return pc.cast(pc.add(as_int, delta_us), pa.timestamp("us"))


def build_trips() -> None:
    raw = download(TRIPS_URL, OUT / "raw" / "yellow_2023-01.parquet")
    t = pq.read_table(raw)
    t = t.rename_columns([c.lower() for c in t.column_names])
    t = t.select(SCHEMA.names).cast(SCHEMA, safe=False)
    # the TLC file contains stray rows from other months; keep January only
    t = t.filter(pc.equal(pc.strftime(t["tpep_pickup_datetime"], format="%Y-%m"), "2023-01"))

    start = date.today() - timedelta(days=31)
    delta_us = (start - date(2023, 1, 1)).days * 86_400 * 1_000_000
    pickup = shift(t["tpep_pickup_datetime"], delta_us)
    dropoff = shift(t["tpep_dropoff_datetime"], delta_us)
    t = t.set_column(t.schema.get_field_index("tpep_pickup_datetime"), "tpep_pickup_datetime", pickup)
    t = t.set_column(t.schema.get_field_index("tpep_dropoff_datetime"), "tpep_dropoff_datetime", dropoff)
    t = t.append_column("dt", pc.cast(pickup, pa.date32()))   # the Hive-style partition column
    pq.write_table(t, OUT / "yellow_trips.parquet")
    print(f"trips: {t.num_rows:,} rows, dates {start} .. {start + timedelta(days=30)}")


def build_zones() -> None:
    raw = download(ZONES_URL, OUT / "raw" / "taxi_zone_lookup.csv")
    t = pacsv.read_csv(raw)
    t = t.rename_columns([c.lower() for c in t.column_names])
    pq.write_table(t, OUT / "taxi_zones.parquet")
    print(f"zones: {t.num_rows} rows")


if __name__ == "__main__":
    build_trips()
    build_zones()
