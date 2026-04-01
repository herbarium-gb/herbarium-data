# hbdb-to-dwc

Pipeline for exporting herbarium data from FileMaker and transforming it to Darwin Core (DwC).

## Overview

- Fetch raw data from FileMaker API
- Transform to Darwin Core
- Export CSV and optional QA tables

## Requirements

```r
install.packages(c(
  "httr",
  "jsonlite",
  "data.table",
  "readxl",
  "sf",
  "readr",
  "writexl"
))
```

## Configuration

Create a `.Renviron` file:

```text
HBDB_API_PWD=your_password_here
FM_BASE_URL=your_base_url_here
```

Restart R after changes.

## Inputs

- Column map: `config/col-map.xlsx`
- Raw data: `data/raw/fm_raw_YYMMDD-HHMMSS.xlsx`

## Run

```r
source("scripts/run_pipeline.R", echo = FALSE)
```

Set in `scripts/run_pipeline.R`:

```r
input_mode <- "file"   # or "fetch"
```

## Outputs

- `data/output/occurrence_YYMMDD-HHMMSS.csv`
- `data/output/bad_projected_coordinates.xlsx` (if needed)

## Notes

- Coordinates derived from: decimal → DMS → SWEREF99 → RT90
- Invalid projected coordinates are excluded and reported
- Duplicate IDs are checked and reported
- Raw and output data are ignored by Git
- `config/col-map.xlsx` is versioned
- `.Renviron` must not be committed
