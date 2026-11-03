from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs" / "tables"


@dataclass
class TableConfig:
    name: str
    source: str                      # dataset.table
    target: str                      # dataset.table
    load_type: str = "full"          # full | incremental
    partition_column: str | None = None
    cluster_by: list[str] = field(default_factory=list)
    numeric_columns: list[str] = field(default_factory=list)


def load_config(name: str, config_dir: Path = CONFIG_DIR) -> TableConfig:
    raw = yaml.safe_load((Path(config_dir) / f"{name}.yaml").read_text())
    return TableConfig(name=name, **raw)
