# Data Engineering Assignment

## Overview

This repository contains solutions for the data engineering assignment given by an edtech startup specializing in building AI mentors. 
It includes two tasks:

1. **Task 1** – Extract text from a PDF and convert it to CSV.
2. **Task 2** – Scrape business articles from a news site(https://indianexpress.com) and store them in SQLite.

All code runs locally or inside a Docker container(The assignment specifically mentioned that the application has to be containarized). 
Dependencies are managed using **uv**.

---

## Motivation

I decided to include this repository as part of my portfolio because I hadn’t previously worked on **PDF conversion** and **extensive web scraping**.  
These assignments gave me the chance to treat them as **mini-projects**, applying data engineering fundamentals, containerization, and structured data handling in practice.  
This makes it a good learning artifact and demonstration of applied problem-solving.


---

## Repository Layout




```
assignment/
├── inputs/                 # Input files (PDF file downloaded and renamed)
├── logs/                   # Empty directory, included for log persistence during docker run
├── outputs/                # Generated output files (submission file & DB, also stores output from docker/local run)
│   ├── db/                 # SQLite database files
│   └── converted_to_pdf_submission.csv
├── source_code/            # Main Python scripts
│   ├── pdf_to_csv.py       # Script for Task 1
│   ├── web_scraper.py      # Script for Task 2
│   └── utils/              # Helper modules
│       ├── DB_helper.py        # Database helper class
│       ├── get_logger.py       # Centralized logging utility
│       └── parameters.py       # Configs & constants
├── pyproject.toml          # Project dependencies
├── uv.lock                 # Locked dependency versions
├── requirements.txt        # Exported requirements for pip users
├── Dockerfile              # Docker build file
├── .gitignore
└── README.md               # Project documentation

```

---

## Requirements

* Python 3.11+
* uv
* Docker

---

## Setup & Usage

### Docker Setup

```bash
# Build the Docker image
docker build -t assignment .

# Run Task 1 (assuming we are in project root directory)
docker run --rm \
  -v $(pwd)/inputs:/assignment/inputs \
  -v $(pwd)/outputs:/assignment/outputs \
  -v $(pwd)/logs:/assignment/logs \
  assignment python pdf_to_csv.py 

# Run Task 2 (assuming we are in project root directory)
docker run --rm \
  -v $(pwd)/inputs:/assignment/inputs \
  -v $(pwd)/outputs:/assignment/outputs \
  -v $(pwd)/logs:/assignment/logs \
  assignment python web_scraper.py -d MM/DD/YYYY
```

The generated CSV/SQLite output will be in `./outputs/`. Logs of corresponding scripts will be at `./logs/`

---

### Local Setup (using uv — uv automatically creates a virtual environment so dependencies are installed there)

```bash
# Install dependencies  
uv sync --frozen

# Run Task 1 
uv run python source_code/pdf_to_csv.py

# Run Task 2 
uv run python source_code/web_scraper.py -d MM/DD/YYYY
```

---

### Local Setup (using pip — be mindful, if you run pip without a virtual environment, dependencies may be installed globally)

```bash
pip install -r requirements.txt
python source_code/pdf_to_csv.py
```

---

## Logging

Logs are stored under the `./logs/` directory.
They include error details and info messages for easier debugging.

---

## Notes

* Only `inputs/` and `outputs/` relevant to the submission are committed.
* CSV output is encoded in **UTF-8-SIG** to preserve special characters in Excel.
* `uv.lock` ensures deterministic builds. Use `uv sync --frozen` to avoid accidental upgrades.
* DB_PATH and log path has to be adjusted for running the code locally.
---

## Author

**Harisudhan Srinivasan**
