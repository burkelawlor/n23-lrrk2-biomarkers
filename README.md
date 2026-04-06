## n23-lrrk2-biomarkers

This repo contains data cleaning, code to run regression analyses, and a Plotly Dash dashboard to display biomarker analysis for **LRRK2-type PD** inferences.

## Run regression (all biomarkers)

`run_biomarker_regression_by_project.py` runs regressions **by project** over the cleaned biospecimen dataset in `data/processed/cleaned_biospecimen_analysis.csv`, and writes:

- `output/biomarker_cohort_omnibus.csv`
- `output/biomarker_cohort_pairwise.csv`

Run for all projects:

```bash
python3 run_biomarker_regression_by_project.py
```

Run for a single project:

```bash
python3 run_biomarker_regression_by_project.py --projectid 145
```

The results in `output/biomarker_cohort_omnibus.csv` are used in the dashboard’s **Overview** page.

## Dashboard

Run the dashboard from the repo root:

```bash
python3 app.py
```

Then open `http://127.0.0.1:8050` in your browser.

## Local SQLite database (recommended for large datasets)

This repo can store cleaned biospecimen rows in a local SQLite file so the app no longer depends on a single giant CSV.

### Build the local DB (also writes CSVs)

From the repo root:

```bash
python3 scripts/clean_biomarkers.py
```

This produces/updates:
- `data/processed/biomarkers.sqlite` (tables: `analysis`, `projects`)
- `data/processed/cleaned_biospecimen_analysis.csv`
- `data/processed/cleaned_biospecimen_projects.csv`

If your raw files live somewhere other than `data/raw`, pass `--data-dir`.

