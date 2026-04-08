# herbarium-data

Scripts for exporting herbarium data from FileMaker, transforming it to Darwin Core (DwC), and optionally loading it into PostgreSQL.

## Overview

- Fetch raw data from FileMaker API
- Transform to Darwin Core
- Export DwC CSV and QA tables
- Optionally load raw and DwC data into PostgreSQL

## Requirements

```r
install.packages(c(
  "httr",
  "jsonlite",
  "data.table",
  "readxl",
  "sf",
  "readr",
  "writexl",
  "DBI",
  "RPostgres"
))
```

## Configuration

Create a `.Renviron` file:

```r
HBDB_API_PWD=your_password_here  
FM_BASE_URL=https://your-filemaker-server  

PGDATABASE=herbarium  
PGUSER=herbarium  
PGPASSWORD=your_password_here  
PGHOST=localhost  
PGPORT=5432  
```

Restart R after changes.

## Run

Run the full pipeline:

```r
source("scripts/run_pipeline.R", echo = FALSE)
```

### Settings in run_pipeline.R

```r
input_mode <- "file"   # "file" or "fetch"  
load_to_db <- FALSE    # TRUE to load into PostgreSQL  
```

## PostgreSQL

To enable database loading:

- A PostgreSQL instance must be running
- Tables `raw.fm_specimen` and `public.dwc_occurrence` must exist
- Connection is configured via `.Renviron`

## Outputs

- data/dwc/occurrence_YYMMDD-HHMMSS.csv
- data/qc/bad_projected_coordinates_YYMMDD-HHMMSS.xlsx (if needed)

## Notes

- Coordinates derived from: decimal → DMS → SWEREF99 → RT90
- Invalid projected coordinates are excluded and reported
- Duplicate IDs are checked and reported
- Data loading replaces all rows in target tables
