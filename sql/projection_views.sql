CREATE OR REPLACE VIEW `PROJECT_ID.taxi_dw.vw_trips_projection` AS
SELECT
  dt AS trip_date,
  vendorid AS vendor_id,
  tpep_pickup_datetime AS pickup_ts,
  tpep_dropoff_datetime AS dropoff_ts,
  pulocationid AS pickup_zone_id,
  dolocationid AS dropoff_zone_id,
  passenger_count,
  trip_distance,
  fare_amount,
  tip_amount,
  total_amount,
  SAFE_DIVIDE(tip_amount, fare_amount) AS tip_ratio
FROM `PROJECT_ID.taxi_dw.yellow_trips`;

CREATE OR REPLACE VIEW `PROJECT_ID.taxi_dw.vw_daily_pickup_summary` AS
SELECT
  t.trip_date,
  z.borough,
  z.zone,
  COUNT(*) AS trips,
  ROUND(SUM(t.total_amount), 2) AS revenue,
  ROUND(AVG(t.trip_distance), 2) AS avg_distance
FROM `PROJECT_ID.taxi_dw.vw_trips_projection` t
LEFT JOIN `PROJECT_ID.taxi_dw.taxi_zones` z ON t.pickup_zone_id = z.locationid
GROUP BY 1, 2, 3;
