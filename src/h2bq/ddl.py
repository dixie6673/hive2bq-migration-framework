from google.cloud import bigquery

TYPE_MAP = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}


def generate_ddl(c, cfg) -> str:
    """Build partitioned + clustered target DDL from the source table's schema."""
    src = c.get_table(cfg.source)
    cols = ",\n".join(
        f"  `{f.name}` {TYPE_MAP.get(f.field_type, f.field_type)}" for f in src.schema
    )
    ddl = f"CREATE TABLE IF NOT EXISTS `{c.project}.{cfg.target}` (\n{cols}\n)"
    if cfg.partition_column:
        ddl += f"\nPARTITION BY `{cfg.partition_column}`"
    if cfg.cluster_by:
        ddl += "\nCLUSTER BY " + ", ".join(f"`{x}`" for x in cfg.cluster_by)
    return ddl


def ensure_target_table(c, cfg) -> None:
    """Create the partitioned + clustered target via the API (same result as the DDL)."""
    src = c.get_table(cfg.source)
    table = bigquery.Table(f"{c.project}.{cfg.target}", schema=src.schema)
    if cfg.partition_column:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=cfg.partition_column)
    if cfg.cluster_by:
        table.clustering_fields = cfg.cluster_by
    c.create_table(table, exists_ok=True)
