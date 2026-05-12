"""
Ingest cleaned CSV artifacts into a PythonAnywhere-hosted MySQL database from your
local machine.

PythonAnywhere MySQL is not reachable from the public internet; you must tunnel over SSH
(paid accounts). See:
https://help.pythonanywhere.com/pages/AccessingMySQLFromOutsidePythonAnywhere/

Usage (recommended: set passwords via environment — never commit them):

  export PA_SSH_PASSWORD='your PythonAnywhere *website* login password'
  export PA_MYSQL_PASSWORD='your MySQL password from the Databases tab'
  python3 scripts/ingest_csv_to_pythonanywhere_mysql.py

If you run a local MySQL on port 3306, set LOCAL_BIND_PORT to something else (e.g. 3333)
and pass --local-bind-port 3333.
"""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path
import sshtunnel
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.db_ingest import load_csv_to_mysql, upsert_clinical_mysql
from utils.db_runtime import create_engine_from_url

load_dotenv(".env")
SSH_HOSTNAME = "ssh.pythonanywhere.com"
SSH_USERNAME = os.getenv("PYTHONANYWHERE_SSH_USERNAME")
MYSQL_REMOTE_HOST = os.getenv("PYTHONANYWHERE_MYSQL_REMOTE_HOST")
MYSQL_DATABASE = os.getenv("PYTHONANYWHERE_MYSQL_DATABASE")
MYSQL_USER = os.getenv("PYTHONANYWHERE_SSH_USERNAME")
SSH_PASSWORD = os.getenv("PYTHONANYWHERE_SSH_PASSWORD")
MYSQL_PASSWORD = os.getenv("PYTHONANYWHERE_MYSQL_PASSWORD")

analysis_csv = str(_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_analysis.csv")
projects_csv = str(_REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_projects.csv")
clinical_csv = _REPO_ROOT / "data" / "processed" / "cleaned_biospecimen_clinical.csv"
chunksize = 100_000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clinical-only", action="store_true", help="Only upsert the clinical table; skip analysis and projects CSVs.")
    args = parser.parse_args()

    sshtunnel.SSH_TIMEOUT = 10.0
    sshtunnel.TUNNEL_TIMEOUT = 10.0

    tunnel_kw: dict = {
        "ssh_address_or_host": (SSH_HOSTNAME, 22),
        "ssh_username": SSH_USERNAME,
        "ssh_password": SSH_PASSWORD,
        "remote_bind_address": (MYSQL_REMOTE_HOST, 3306),
    }
    with sshtunnel.SSHTunnelForwarder(**tunnel_kw) as tunnel:        
        port = tunnel.local_bind_port
        print(f"SSH tunnel established, local bind port: {port}")

        database_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@127.0.0.1:{port}/{MYSQL_DATABASE}"
        print(f"Connecting to database at URL: {database_url}")

        engine = create_engine_from_url(database_url)
        print("Engine created, starting CSV ingestion...")

        import pandas as pd
        clinical_df = pd.read_csv(clinical_csv, low_memory=False)
        upsert_clinical_mysql(engine, clinical_df)
        print(f"Upserted {len(clinical_df)} clinical rows.")

        if not args.clinical_only:
            load_csv_to_mysql(
                engine,
                analysis_csv_path=analysis_csv,
                projects_csv_path=projects_csv,
                chunksize=chunksize,
            )
        print("Ingestion complete (PythonAnywhere MySQL via SSH tunnel).")


if __name__ == "__main__":
    main()
