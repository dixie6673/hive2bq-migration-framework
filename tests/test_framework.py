from types import SimpleNamespace as NS

from h2bq.config import TableConfig
from h2bq.ddl import generate_ddl
from h2bq.validator import _same


class FakeClient:
    project = "demo"

    def get_table(self, ref):
        return NS(schema=[NS(name="a", field_type="INTEGER"), NS(name="dt", field_type="DATE")])


def test_ddl_has_partition_cluster_and_mapped_types():
    cfg = TableConfig(name="t", source="s.t", target="d.t", load_type="incremental",
                      partition_column="dt", cluster_by=["a"])
    ddl = generate_ddl(FakeClient(), cfg)
    assert "`a` INT64" in ddl
    assert "PARTITION BY `dt`" in ddl
    assert "CLUSTER BY `a`" in ddl


def test_same_handles_none_and_float_tolerance():
    assert _same(None, None)
    assert not _same(None, 0)
    assert _same(1.0000000001, 1.0)
    assert not _same(10, 11)
