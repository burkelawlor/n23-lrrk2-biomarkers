from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.db_ingest import load_csv_to_mysql
from utils.db_runtime import create_engine_from_url


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Create schema and ingest cleaned CSV artifacts into a MySQL database "
            "using DATABASE_URL (direct TCP). For PythonAnywhere from your laptop, "
            "MySQL is firewalled; use scripts/ingest_csv_to_pythonanywhere_mysql.py instead."
        )
    )
    p.add_argument(
        "--analysis-csv",
        type=str,
        default=str(_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_analysis.csv"),
        help="Path to cleaned analysis CSV.",
    )
    p.add_argument(
        "--projects-csv",
        type=str,
        default=str(_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv"),
        help="Path to cleaned projects CSV.",
    )
    p.add_argument(
        "--database-url",
        type=str,
        default=None,
        help="Override DATABASE_URL from the environment.",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=50_000,
        help="Rows per chunk to stream from CSV (default: 50000).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    database_url = args.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("Missing DATABASE_URL (set env var or pass --database-url).")

    engine = create_engine_from_url(database_url)
    load_csv_to_mysql(
        engine,
        analysis_csv_path=args.analysis_csv,
        projects_csv_path=args.projects_csv,
        chunksize=args.chunksize,
    )
    print("Ingestion complete.")


if __name__ == "__main__":
    main()

