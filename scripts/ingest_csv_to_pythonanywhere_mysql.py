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
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus
import sshtunnel

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.db_ingest import load_csv_to_mysql
from utils.db_runtime import create_engine_from_url

# --- PythonAnywhere account / DB (edit region if needed) ---
# Global US: ssh.pythonanywhere.com | EU: ssh.eu.pythonanywhere.com
SSH_HOSTNAME = "ssh.pythonanywhere.com"
SSH_USERNAME = "blawlor"
# Remote MySQL hostname from the Databases tab (not the SSH host).
MYSQL_REMOTE_HOST = "blawlor.mysql.pythonanywhere-services.com"
MYSQL_REMOTE_PORT = 3306
# Full database name: username$database — URL-encode $ when building the URL.
MYSQL_DATABASE = "blawlor$biomarkers"
# MySQL user is usually your PythonAnywhere username (set MYSQL_USER if different).
MYSQL_USER = "blawlor"

# Placeholders — set PA_SSH_PASSWORD and PA_MYSQL_PASSWORD in the environment.
PA_SSH_PASSWORD_PLACEHOLDER = "pgYLrkK?gSR$3ktA"
PA_MYSQL_PASSWORD_PLACEHOLDER = "9FFg&h?C44R3Smdp"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest CSVs into PythonAnywhere MySQL via SSH tunnel (from local machine)."
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
        "--chunksize",
        type=int,
        default=50_000,
        help="Rows per chunk when reading the analysis CSV.",
    )
    p.add_argument(
        "--ssh-hostname",
        type=str,
        default=SSH_HOSTNAME,
        help="SSH server (US: ssh.pythonanywhere.com, EU: ssh.eu.pythonanywhere.com).",
    )
    p.add_argument(
        "--local-bind-port",
        type=int,
        default=0,
        help="Local port for the tunnel (0 = let sshtunnel pick a free port).",
    )
    return p.parse_args()


def _require_secret(env_name: str, placeholder: str) -> str:
    value = os.environ.get(env_name, "").strip()
    if not value or value == placeholder:
        raise SystemExit(
            f"Missing or unset {env_name}. Set it to your secret (do not commit it). "
            f"Example: export {env_name}='...'"
        )
    return value


def main() -> None:
    args = _parse_args()

    ssh_password = _require_secret("PA_SSH_PASSWORD", PA_SSH_PASSWORD_PLACEHOLDER)
    mysql_password = _require_secret("PA_MYSQL_PASSWORD", PA_MYSQL_PASSWORD_PLACEHOLDER)

    # Match PythonAnywhere docs defaults for tunnel timeouts.
    sshtunnel.SSH_TIMEOUT = 10.0
    sshtunnel.TUNNEL_TIMEOUT = 10.0

    tunnel_kw: dict = {
        "ssh_username": SSH_USERNAME,
        "ssh_password": ssh_password,
        "remote_bind_address": (MYSQL_REMOTE_HOST, MYSQL_REMOTE_PORT),
    }
    # If local MySQL already uses 3306, set e.g. --local-bind-port 3333
    if args.local_bind_port:
        tunnel_kw["local_bind_address"] = ("127.0.0.1", args.local_bind_port)

    with sshtunnel.SSHTunnelForwarder((args.ssh_hostname, 22), **tunnel_kw) as tunnel:
        port = tunnel.local_bind_port
        # Database name contains '$' — must be encoded in the URL.
        user_q = quote_plus(MYSQL_USER)
        pass_q = quote_plus(mysql_password)
        db_q = quote_plus(MYSQL_DATABASE)
        database_url = f"mysql+pymysql://{user_q}:{pass_q}@127.0.0.1:{port}/{db_q}"

        engine = create_engine_from_url(database_url)
        load_csv_to_mysql(
            engine,
            analysis_csv_path=args.analysis_csv,
            projects_csv_path=args.projects_csv,
            chunksize=args.chunksize,
        )
        print("Ingestion complete (PythonAnywhere MySQL via SSH tunnel).")


if __name__ == "__main__":
    main()
