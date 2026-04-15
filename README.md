# Neuron23 LRRK2 Biomarkers

This repo contains data cleaning, code to run regression analyses, and a Plotly Dash dashboard to display biomarker analysis for **LRRK2-type PD** inferences.

## Running locally

Make sure MySQL is running. The cleaner will use `DATABASE_URL` from your environment (recommended: store it in `.env` in the repo root).

```bash
docker compose up -d
```

### 1) Clean biomarkers

From the repo root:

```bash
python3 scripts/clean_biomarkers.py
```

Run for a single project with the flag `--project-id`. By default, `scripts/clean_biomarkers.py` loads `.env` and uses `DATABASE_URL` (if set) to also load the cleaned CSV artifacts into MySQL. Override the DB destination by setting `--database-url` to a valid url, or skip DB loading entierly by passing "". 


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

PythonAnywhere MySQL is not reachable from the public internet; use an SSH tunnel (paid accounts). Store the PythonAnywhere environment variables in `.env` (see [Accessing your MySQL database from outside PythonAnywhere](https://help.pythonanywhere.com/pages/AccessingMySQLFromOutsidePythonAnywhere/)). 

Then run:

```bash
python3 scripts/ingest_csv_to_pythonanywhere_mysql.py
```

If you already use port `3306` locally for MySQL, add `--local-bind-port 3333` and ensure nothing else is bound there.

### 2) Manage the web app in the PythonAnywhere console

Use the PythonAnywhere web UI/console to run the app, reload the web app, and check logs. (The dashboard reads from the PythonAnywhere MySQL database configured there.)