import math
import uuid
from datetime import datetime, timezone

from .bq import append_rows, ensure_meta, fq


class DVTFailure(Exception):
    pass


def _row(c, sql: str) -> dict:
    return dict(next(iter(c.query(sql).result())).items())


def _same(a, b) -> bool:
    if a is None or b is None:
        return a == b
    if isinstance(a, float) or isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-6)
    return a == b


def run_dvt(c, cfg) -> None:
    ensure_meta(c)
    src, tgt = fq(c, cfg.source), fq(c, cfg.target)
    cols = [f.name for f in c.get_table(cfg.source).schema]
    run_id, ts = uuid.uuid4().hex[:8], datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    def add(check, detail, sv, tv, ok):
        results.append({"run_id": run_id, "run_ts": ts, "table_name": cfg.name, "check_name": check,
                            "detail": str(detail), "source_value": str(sv), "target_value": str(tv),
                            "status": "PASS" if ok else "FAIL"})

    # 1) row count, null counts, numeric aggregates
    parts = ["COUNT(*) AS row_count"]
    parts += [f"COUNTIF(`{x}` IS NULL) AS nulls__{x}" for x in cols]
    for n in cfg.numeric_columns:
        parts += [f"SUM(`{n}`) AS sum__{n}", f"MIN(`{n}`) AS min__{n}", f"MAX(`{n}`) AS max__{n}"]
    sql = "SELECT " + ", ".join(parts) + " FROM `{}`"
    s, t = _row(c, sql.format(src)), _row(c, sql.format(tgt))
    for key in s:
        check, _, col = key.partition("__")
        add(check, col, s[key], t[key], _same(s[key], t[key]))

    # 2) per-partition count + row-hash checksum
    pc = cfg.partition_column
    k_expr = f"CAST(`{pc}` AS STRING)" if pc else "'ALL'"
    grp = "GROUP BY k" if pc else ""
    hsql = (f"SELECT {k_expr} AS k, COUNT(*) AS n, "
            f"BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS h FROM `{{}}` t {grp}")
    sh = {r.k: (r.n, r.h) for r in c.query(hsql.format(src)).result()}
    th = {r.k: (r.n, r.h) for r in c.query(hsql.format(tgt)).result()}
    bad = sorted(k for k in sh.keys() | th.keys() if sh.get(k) != th.get(k))
    add("partition_checksum", f"{len(bad)} mismatching of {len(sh)} partitions",
        len(sh), len(th), not bad)
    for k in bad[:20]:
        add("partition_mismatch", k, sh.get(k), th.get(k), False)

    append_rows(c, "validation_results", results)

    failed = [r for r in results if r["status"] == "FAIL"]
    print(f"[{cfg.name}] DVT run {run_id}: {len(results) - len(failed)} passed, {len(failed)} failed")
    for r in failed:
        print(f"   FAIL {r['check_name']} {r['detail']}: source={r['source_value']} target={r['target_value']}")
    if failed:
        raise DVTFailure(f"{cfg.name}: {len(failed)} validation checks failed (run {run_id})")
