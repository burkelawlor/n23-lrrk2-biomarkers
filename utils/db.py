"""
Compatibility wrapper.

The repo has moved to a MySQL-first workflow with `utils/db_runtime.py` and
`utils/db_ingest.py`. This module remains as a thin import shim for any older
callers that still import `utils.db`.
"""

from utils.db_runtime import (  # noqa: F401
    create_engine_from_url,
    fetch_analysis_subset,
    get_engine_from_env,
    get_project_rundates_lookup,
    get_projects_df,
    get_projects_lookup,
    get_testnames,
)

from utils.db_ingest import (  # noqa: F401
    init_schema,
    insert_analysis_ignore_duplicates_mysql,
    load_csv_to_mysql,
    upsert_projects_mysql,
)

