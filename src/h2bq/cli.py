import argparse
from datetime import date
from pathlib import Path

from .bq import client
from .config import load_config
from .ddl import generate_ddl
from .loader import run_load
from .validator import run_dvt


def main() -> None:
    p = argparse.ArgumentParser(prog="h2bq")
    p.add_argument("command", choices=["ddl", "load", "validate"])
    p.add_argument("--table", required=True)
    p.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD (incremental tables)")
    p.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD (incremental tables)")
    a = p.parse_args()

    c, cfg = client(), load_config(a.table)
    if a.command == "ddl":
        ddl = generate_ddl(c, cfg)
        out = Path("ddl")
        out.mkdir(exist_ok=True)
        (out / f"{cfg.name}.sql").write_text(ddl + ";\n")
        print(ddl)
    elif a.command == "load":
        run_load(c, cfg, a.start, a.end)
    else:
        run_dvt(c, cfg)


if __name__ == "__main__":
    main()
