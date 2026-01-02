# IBD – SINISA PoC  
**Building a Transparent, Reproducible, and Verifiable Pipeline for Brazilian Water Sector Data**

---

## Project Overview

This repository contains a fully transparent and reproducible pipeline developed for a Proof of Concept (PoC) with the **Inter-American Development Bank (IDB)** using **SINISA – Brazil’s National Sanitation Information System** water supply data.

The objective is to:

- Retrieve **official public data**
- Prepare curated analytical datasets
- Compute official indicators reproducibly
- Validate against official published metrics
- Document methodology transparently
- Deliver a structured and auditable workflow
- Publish a Quarto presentation (GitHub Pages)

Everything is **open, verifiable, replicable, and auditable**.

---

## Repository Structure

```
.
├── data/
│   ├── raw/            # Downloaded files (zip + extracted)
│   └── processed/      # Clean datasets produced by the pipeline
│
├── scripts/
│   ├── 01_download_and_unzip_sinisa.R
│   ├── 02_prepare_sinisa_datasets.R
│   ├── 03_compare_sinisa_ibge.R
│   └── 04_prepare_literature_matrix.R
│
├── logs/               # Structured logs for transparency
│
├── quarto/
│   ├── bid_sinisa_poc_presentation.qmd
│   ├── src/
│   │   └── rct_fivenum.R
│   └── _quarto.yml
│
├── docs/
│   ├── index.html      # Published presentation (GitHub Pages)
│   └── .nojekyll       # https://quarto.org/docs/publishing/github-pages.html
│
└── README.md
```

---

## Data Source

All data in this project originates from:

Brazilian Ministry of Cities – SINISA  
Official public repository of water services indicators.

> https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/sinisa

No private or confidential data is used.
Everything is **open access**.

---

## Pipeline Summary

### 1. Download & Extract Official Data  
Automatically retrieves the official SINISA water dataset and safely unzips it (handling encoding issues and Windows/Mac/Linux compatibility).

Output → `data/raw/`

---

### 2. Prepare Analytical Datasets  
Reads Excel sheets, standardizes headers, identifies structure rules, extracts metadata and builds:
- core municipal dataset
- variable dictionary
- index metadata
- statistics summaries
- validation files

Output → `data/processed/`

---

### 3. IBGE Cross-Validation
This step validates SINISA municipal information using an official IBGE municipal reference table.
Checks:

- Municipality codes (cod_IBGE)

- Municipality names

- UF and macroregion consistency

---

### 4. Index Computation and Validation
- Computes SINISA indexes reproducibly
- Handles exceptions & rules via expression engine
- Logs every expression, success, failure, and exception impact
- Compares official vs computed
- Produces validation and summary tables

---

### 5. Quarto Presentation
A fully automated Quarto Reveal.js presentation is built and published to GitHub Pages.

Access:
```
https://diegomiro.github.io/idb-sinisa-poc/
```

---

## How to Reproduce the Full Pipeline

### 1. Install Dependencies

```r
install.packages(c(
  "tidyverse", "fs", "readxl", "janitor",
  "rlang", "purrr", "logger", "sf"
))
```

---

### 2. Run Pipeline Steps in Order

```r
source("R/01_download_unzip_sinisa.R")
source("R/02_prepare_and_compute_sinisa.R")
source("R/03_build_ibge_municipal_table.R")
source("R/03_compare_sinisa_ibge.R")
source("R/04_build_literature_matrix.R")
```

---

### 3. Build the Presentation

```bash
cd quarto
quarto render
```

Output is automatically written to:

```
/docs/index.html
```

---

## Logging & Auditability

Every critical step of the pipeline:
- logs execution
- records errors / warnings
- tracks exception rules applied
- registers successes and failures

Logs live in:

```
logs/
```

Meaning:
- Anyone can verify what happened
- Decisions are documented
- Pipeline is auditable

This is **research-grade reproducibility**.

---

## Outputs Delivered

- Final analytical datasets  
- Variable dictionary  
- Index metadata  
- Computed Indexes  
- Official vs Computed validation  
- Exception rule audit log  
- Statistics for indicators  
- Quarto Presentation

---

## Why This Matters

This project demonstrates:

- Technical capability and data skills
- Sector and institutional understanding
- Alignment with the Terms of Reference
- Reproducibility & transparency
- Professional documentation standards

This is not “just code”.
It is a **complete, credible analytical product**.

---

## Author

**Diego Miro**  
Statistician & Data Scientist  
Brazil
