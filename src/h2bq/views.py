from pathlib import Path

SQL_FILE = Path(__file__).resolve().parents[2] / "sql" / "projection_views.sql"


def create_views(c) -> None:
    c.query(SQL_FILE.read_text().replace("PROJECT_ID", c.project)).result()
    print("projection views created")
