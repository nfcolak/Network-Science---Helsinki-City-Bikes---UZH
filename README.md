# Helsinki City Bikes Network Science

This repository analyzes Helsinki City Bikes trips from April 2021 as a directed station network.

The prepared data has 258,514 cleaned trips and 351 stations. The notebooks build weighted station edges, calculate network measures, compare null models, inspect station imbalance, and generate maps and plots.

## What is here

```text
.
├── data/
│   ├── raw/                  # local source CSVs, ignored by git
│   ├── processed/            # cleaned trip tables used by notebooks
│   ├── reference/            # station coordinate cache
│   ├── derived/              # generated CSV summaries
│   └── figures/              # generated images
├── notebooks/                # analysis notebooks
├── scripts/                  # data preparation and checks
├── requirements.txt
└── README.md
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Data files

The repo includes prepared files under `data/processed`, `data/reference`, `data/derived`, and `data/figures`.

Raw trip data stays local. Put the April 2021 source CSV in one of these locations:

```text
data/raw/2021-04.csv
data/raw/2021-04 (2).csv
```

## Rebuild prepared data

Run these commands from the repository root:

```bash
python scripts/geocode_stations.py
python scripts/clean_data.py
python scripts/merge_data.py
```

`scripts/geocode_stations.py` uses the cached coordinate file when it already exists. Pass `--overwrite` if you want to rebuild `data/reference/geocode_cache.csv`.

## Notebook order

1. `notebooks/02_graph_construction.ipynb`
2. `notebooks/centrality_measures/degree_centrality.ipynb`
3. `notebooks/temporal_analysis.ipynb`
4. `notebooks/07_null_model_hypothesis_testing.ipynb`
5. `notebooks/08_advanced_analysis.ipynb`
6. `notebooks/09_network_robustness.ipynb`
7. `notebooks/visuals_network.ipynb`

The notebooks can run from the repository root or from inside `notebooks/`.

## Useful script commands

```bash
python scripts/analyze_data.py
python scripts/clean_data.py --input data/raw/2021-04.csv
python scripts/merge_data.py
python scripts/geocode_stations.py --help
```

Use `--help` on any script to see its input and output options.
