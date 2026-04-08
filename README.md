# Neuron23 LRRK2 Biomarkers

This repo contains data cleaning, code to run regression analyses, and a Plotly Dash dashboard to display biomarker analysis for **LRRK2-type PD** inferences.

## Running locally

Make sure MySQL is running and `DATABASE_URL` is set:

```bash
docker compose up -d
export DATABASE_URL="mysql+pymysql://biomarkers:biomarkers@127.0.0.1:3306/biomarkers"
```

### 1) Clean biomarkers

From the repo root:

```bash
python3 scripts/clean_biomarkers.py --database-url "$DATABASE_URL"
```

`--database-url` tells the cleaner where to write cleaned rows (and enables DB-backed workflows, instead of only producing CSV outputs).

This produces/updates:

- `data/processed/cleaned_biospecimen_analysis.csv`
- `data/processed/cleaned_biospecimen_projects.csv`

If your raw files live somewhere other than `data/raw`, pass `--data-dir`.

### 2) Run regressions

`scripts/run_biomarker_regression_by_project.py` runs regressions **by project** over `data/processed/cleaned_biospecimen_analysis.csv`, and writes:

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

### 3) Run dashboard

From the repo root:

```bash
python3 app.py
```

Then open `http://127.0.0.1:8050` in your browser.

## Running in PythonAnywhere

### 1) Ingest cleaned biomarkers to the PythonAnywhere database (from your computer)

PythonAnywhere MySQL is not reachable from the public internet; use an SSH tunnel (paid accounts). Pythonanywhere environment variable should be set in stored in `.env.local` (se [Accessing your MySQL database from outside PythonAnywhere](https://help.pythonanywhere.com/pages/AccessingMySQLFromOutsidePythonAnywhere/)). 

Then run:

```bash
python3 scripts/ingest_csv_to_pythonanywhere_mysql.py
```

If you already use port `3306` locally for MySQL, add `--local-bind-port 3333` and ensure nothing else is bound there.

### 2) Manage the web app in the PythonAnywhere console

Use the PythonAnywhere web UI/console to run the app, reload the web app, and check logs. (The dashboard reads from the PythonAnywhere MySQL database configured there.)