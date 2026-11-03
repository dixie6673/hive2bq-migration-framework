from google.cloud import bigquery

REGION = "asia-south1"
META = "dvt_audit"
SF = bigquery.SchemaField


def client() -> bigquery.Client:
    return bigquery.Client(location=REGION)


def fq(c: bigquery.Client, ref: str) -> str:
    """dataset.table -> project.dataset.table"""
    return f"{c.project}.{ref}"


def ensure_meta(c: bigquery.Client) -> None:
    c.create_table(bigquery.Table(f"{c.project}.{META}.load_watermark", schema=[
        SF("table_name", "STRING"), SF("last_loaded_dt", "DATE"),
        SF("rows_in_target", "INT64"), SF("updated_at", "TIMESTAMP")]), exists_ok=True)
    c.create_table(bigquery.Table(f"{c.project}.{META}.validation_results", schema=[
        SF("run_id", "STRING"), SF("run_ts", "TIMESTAMP"), SF("table_name", "STRING"),
        SF("check_name", "STRING"), SF("detail", "STRING"), SF("source_value", "STRING"),
        SF("target_value", "STRING"), SF("status", "STRING")]), exists_ok=True)


def append_rows(c: bigquery.Client, meta_table: str, rows: list[dict]) -> None:
    """Append rows with a load job (the Sandbox has no INSERT/streaming)."""
    ref = f"{c.project}.{META}.{meta_table}"
    cfg = bigquery.LoadJobConfig(schema=c.get_table(ref).schema, write_disposition="WRITE_APPEND")
    c.load_table_from_json(rows, ref, job_config=cfg).result()
