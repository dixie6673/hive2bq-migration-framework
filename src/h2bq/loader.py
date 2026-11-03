from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

from .bq import META, append_rows, ensure_meta, fq
from .ddl import ensure_target_table


def _param(name, kind, value):
    return bigquery.ScalarQueryParameter(name, kind, value)


def get_watermark(c, name: str):
    sql = (f"SELECT last_loaded_dt FROM `{c.project}.{META}.load_watermark` "
           "WHERE table_name = @n ORDER BY updated_at DESC LIMIT 1")
    rows = list(c.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[_param("n", "STRING", name)])).result())
    return rows[0].last_loaded_dt if rows else None


def pending_dates(c, cfg, start: date, end: date) -> list[date]:
    sql = (f"SELECT DISTINCT `{cfg.partition_column}` AS d FROM `{fq(c, cfg.source)}` "
           f"WHERE `{cfg.partition_column}` BETWEEN @s AND @e ORDER BY d")
    job = c.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[_param("s", "DATE", start), _param("e", "DATE", end)]))
    return [r.d for r in job.result()]


def load_partition(c, cfg, d: date) -> None:
    """Overwrite exactly one target partition: idempotent, and no DML needed."""
    c.query(
        f"SELECT * FROM `{fq(c, cfg.source)}` WHERE `{cfg.partition_column}` = @dt",
        job_config=bigquery.QueryJobConfig(
            destination=f"{c.project}.{cfg.target}${d:%Y%m%d}",
            write_disposition="WRITE_TRUNCATE",
            query_parameters=[_param("dt", "DATE", d)]),
    ).result()


def run_load(c, cfg, start: date | None = None, end: date | None = None) -> None:
    ensure_meta(c)

    if cfg.load_type == "full":
        c.query(f"SELECT * FROM `{fq(c, cfg.source)}`",
                job_config=bigquery.QueryJobConfig(
                    destination=f"{c.project}.{cfg.target}",
                    write_disposition="WRITE_TRUNCATE")).result()
        print(f"[{cfg.name}] full refresh complete")
        return

    ensure_target_table(c, cfg)
    wm = get_watermark(c, cfg.name)
    s = start or (wm + timedelta(days=1) if wm else date(1970, 1, 1))
    e = end or date(2100, 1, 1)
    dates = pending_dates(c, cfg, s, e)
    if not dates:
        print(f"[{cfg.name}] nothing new to load (watermark={wm})")
        return

    for i, d in enumerate(dates, 1):
        load_partition(c, cfg, d)
        print(f"[{cfg.name}] loaded {d} ({i}/{len(dates)})")

    # a manual repair of an old partition must not move the watermark backwards
    new_wm = max(dates) if wm is None else max(wm, max(dates))
    total = list(c.query(f"SELECT COUNT(*) AS n FROM `{fq(c, cfg.target)}`").result())[0].n
    append_rows(c, "load_watermark", [{
        "table_name": cfg.name, "last_loaded_dt": new_wm.isoformat(),
        "rows_in_target": total,
        "updated_at": datetime.now(timezone.utc).isoformat()}])
    print(f"[{cfg.name}] done. watermark={new_wm} rows_in_target={total:,}")
