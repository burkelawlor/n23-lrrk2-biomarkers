## n23-lrrk2-biomarkers

This repo contains data cleaning, code to run regression analyses, and a Plotly Dash dashboard to display biomarker analysis for **LRRK2-type PD** inferences.

## Run regression (all biomarkers)

`scripts/run_biomarker_regression_by_project.py` runs regressions **by project** over the cleaned biospecimen dataset in `data/processed/cleaned_biospecimen_analysis.csv`, and writes:

- `output/biomarker_cohort_omnibus.csv`
- `output/biomarker_cohort_pairwise.csv`

Run for all projects:

```bash
python3 scripts/run_biomarker_regression_by_project.py
```

Run for a single project:

```bash
python3 scripts/run_biomarker_regression_by_project.py --projectid 145
```

The results in `output/biomarker_cohort_omnibus.csv` are used in the dashboard’s **Overview** page.

## Dashboard

Run the dashboard from the repo root:

```bash
export DATABASE_URL="mysql+pymysql://biomarkers:biomarkers@127.0.0.1:3306/biomarkers"
```

```bash
python3 app.py
```

Then open `http://127.0.0.1:8050` in your browser.

## Local MySQL database (recommended for large datasets)

This repo can store cleaned biospecimen rows in a local MySQL database so the app no longer depends on a single giant CSV.

### Start local MySQL (Docker)

From the repo root:

```bash
docker compose up -d
```

Then set `DATABASE_URL` for local development:

```bash
export DATABASE_URL="mysql+pymysql://biomarkers:biomarkers@127.0.0.1:3306/biomarkers"
```

### Build the local artifacts (also writes CSVs)

From the repo root:

```bash
python3 scripts/clean_biomarkers.py
```

This produces/updates:
- `data/processed/cleaned_biospecimen_analysis.csv`
- `data/processed/cleaned_biospecimen_projects.csv`

If your raw files live somewhere other than `data/raw`, pass `--data-dir`.

### Ingest CSVs into local / direct MySQL

This script connects with `DATABASE_URL` over the network (e.g. Docker MySQL on `127.0.0.1`):

```bash
python3 scripts/ingest_csv_to_mysql.py
```

### Ingest CSVs into PythonAnywhere MySQL (from your computer)

PythonAnywhere MySQL is not reachable from the public internet; use an SSH tunnel (paid accounts). See [Accessing your MySQL database from outside PythonAnywhere](https://help.pythonanywhere.com/pages/AccessingMySQLFromOutsidePythonAnywhere/).

Set your **website login** password and your **MySQL** password (from the Databases tab), then run:

```bash
export PA_SSH_PASSWORD='your PythonAnywhere website password'
export PA_MYSQL_PASSWORD='your MySQL password from the Databases tab'
python3 scripts/ingest_csv_to_pythonanywhere_mysql.py
```

If you already use port `3306` locally for MySQL, add `--local-bind-port 3333` and ensure nothing else is bound there.

