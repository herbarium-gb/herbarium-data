# herbarium-data

Pipeline for publishing the Gothenburg herbarium (GB) specimen collection to
[GBIF](https://www.gbif.org/).

Specimen records live in FileMaker. This repository fetches them, transforms
them to [Darwin Core](https://dwc.tdwg.org/), loads the result into a
PostgreSQL "publication layer", and triggers an
[IPT](https://www.gbif.org/ipt) to publish a new version of the dataset.
GBIF then harvests the IPT on its own schedule.

```
FileMaker  ──►  Darwin Core CSV  ──►  PostgreSQL  ──►  IPT version  ──►  GBIF
 (fetch)         (transform)          (load)           (publish)        (harvest)
```

The image server, web viewer, and the PostgreSQL/IPT deployment are managed
separately in **[herbarium-platform](https://github.com/herbarium-gb/herbarium-platform)**.
This repository only produces the data and asks the IPT to publish it.

## Quick start

Assumes `.Renviron`, the SSH tunnel, and the IPT resource are already set up
(see the sections below for first-time setup).

1. If running off-server, open the database tunnel in its own terminal:
   `ssh -N herbarium-db`
2. Open the project in RStudio and restart R, so `.Renviron` is loaded.
3. Set the switches at the top of `scripts/run_pipeline.R`, e.g.:
   ```r
   input_mode  <- "fetch"   # pull fresh data from FileMaker
   target      <- "test"    # publish to the test IPT
   load_to_db  <- TRUE
   publish_ipt <- TRUE
   ```
4. Run it with the **Source** button (not line by line, or the prompts
   answer themselves).
5. Answer the prompts: `y` past any QA warnings, then `y` (test) or `PROD`
   (production) to publish.

Afterwards, check the new version and its record count on the IPT.

## Pipeline

`scripts/run_pipeline.R` runs the stages in order; each stage is also a
standalone script.

| Stage | Script | What it does |
|-------|--------|--------------|
| Fetch | `fetch_fm_data.R` | Pulls all records from the FileMaker Data API, writes `data/raw/fm_raw_*.xlsx`. Aborts if the API returns 0 rows. |
| Transform | `transform_to_dwc.R` | Maps source fields to Darwin Core using `config/col-map.xlsx`, derives coordinates and `eventDate`, joins Digital Specimen DOIs if `config/dissco_dois.csv` is present, writes `data/dwc/occurrence_*.csv` and (if there are issues) `data/qc/qa_*.xlsx`. |
| Load | `load_to_postgres.R` | Replaces `raw.fm_specimen` and `public.dwc_occurrence` with the latest raw and DwC files. Refuses to load an empty file. |
| Publish | `publish_ipt.R` | Logs in to the IPT, publishes a new resource version, polls until it finishes, and checks the published record count. |

`scripts/fetch_dissco_dois.R` is a separate, occasional job — see **Digital
Specimen DOIs** below.

## Requirements

R (4.x) with:

```r
install.packages(c(
  "httr", "jsonlite", "data.table", "readxl",
  "sf", "readr", "writexl", "DBI", "RPostgres"
))
```

## Configuration

Create a `.Renviron` file in the project root:

```r
# FileMaker Data API
HBDB_API_USR=api
HBDB_API_PWD=your_filemaker_password
FM_BASE_URL=https://your-filemaker-server/fmi/data/vLatest/databases/<db>

# PostgreSQL publication layer
PGDATABASE=herbarium
PGUSER=herbarium
PGPASSWORD=your_postgres_password
PGHOST=localhost
PGPORT=5433

# IPT - one block per target
IPT_TEST_BASE_URL=https://test.gbif.se/ipt
IPT_TEST_RESOURCE=gb_herbarium
IPT_TEST_USER=you@example.org
IPT_TEST_PASS=your_test_ipt_password

IPT_PROD_BASE_URL=https://www.gbif.se/ipt
IPT_PROD_RESOURCE=gb_herbarium
IPT_PROD_USER=you@example.org
IPT_PROD_PASS=your_prod_ipt_password
```

Restart R after editing `.Renviron` — it is only read at startup.

`.Renviron` holds passwords. It is git-ignored; also run `chmod 600 .Renviron`
so only your account can read it. `HBDB_API_USR` defaults to `api` if unset.

## Running the pipeline

Open the project and use the **Source** button on `scripts/run_pipeline.R`, or:

```r
source("scripts/run_pipeline.R", echo = FALSE)
```

Run it via Source (not by stepping through the lines) so the confirmation
prompts wait for your answer.

### Settings

Edit the top of `run_pipeline.R`:

```r
input_mode  <- "file"   # "file" = use the latest data/raw file; "fetch" = pull from FileMaker
target      <- "test"   # "test" or "prod" - which IPT to publish to
load_to_db  <- FALSE    # TRUE writes to PostgreSQL (TRUNCATE + reload both tables)
check_media <- FALSE    # TRUE opens every associatedMedia URL to check it (slow: one request per record)
publish_ipt <- FALSE    # TRUE publishes a new IPT version
```

`target` selects which `IPT_TEST_*` / `IPT_PROD_*` block from `.Renviron` is used.

### Prompts

- **QA gate** — if the transform found duplicate `occurrenceID`s, invalid
  projected coordinates, or broken media links, the publish step lists them
  and asks whether to continue.
- **Publish confirmation** — `test` asks for a plain `y`; any IPT whose URL
  does not contain `test` is treated as production and asks you to type
  `PROD` (exactly, uppercase).

## FileMaker Data API

The fetch stage reads records through the FileMaker Data API. Three things
must be set up once on the FileMaker side:

1. **Data API enabled** on FileMaker Server (Admin Console → Connectors).
2. **An API account** on the hosted file whose privilege set has the
   `fmrest` extended privilege. `fetch_fm_data.R` logs in as `HBDB_API_USR`
   (default `api`) with `HBDB_API_PWD`.
3. **A layout** named `GBIF_export`, visible to that account, exposing the
   source fields the transform expects (accession number, collector, the
   coordinate fields, year / month / day, and so on).

At runtime the script opens a session (`POST /sessions`), pages through
`/layouts/GBIF_export/records` 1000 records at a time, and logs out on exit.

If it fails: an error at `POST /sessions` points to the account, its
`fmrest` privilege, or the server toggle; an error once logged in points to
the `GBIF_export` layout.

## PostgreSQL access

The pipeline and the IPT both read the same PostgreSQL database. It is a
staging layer only — its contents are replaced on every load.

If you run R on the database server itself, no tunnel is needed — set
`PGPORT=5432`.

If you run R on your own machine, reach the database over an SSH tunnel that
forwards local port `5433` to port `5432` on the server (`5433` avoids
clashing with a local PostgreSQL on `5432`). Run it in its own terminal and
leave it open:

```bash
ssh -N -L 5433:localhost:5432 user@your-server
```

Or add a `~/.ssh/config` entry (ask the maintainer for host and user) and
start it by name:

```
Host herbarium-db
  HostName <database server>
  User <your user>
  IdentityFile ~/.ssh/<your key>
  LocalForward 5433 localhost:5432
```

```bash
ssh -N herbarium-db
```

Then set `PGHOST=localhost` and `PGPORT=5433` in `.Renviron`.

`load_to_postgres.R` needs the tables `raw.fm_specimen` and
`public.dwc_occurrence` to already exist.

## IPT publishing

`publish_ipt.R` mimics the IPT web UI (there is no REST API): it fetches a
CSRF token, logs in with a form + session cookie, POSTs `manage/publish.do`,
then polls `manage/report.do` until the publication finishes.

It does **not** configure the resource. Before it can publish anything, the
IPT resource must already have, set up by hand in the IPT:

- a **source** (the PostgreSQL connection),
- a complete **Darwin Core mapping** (Occurrence core),
- a **publishing organisation**,
- the mandatory metadata fields.

A resource missing any of these publishes an empty archive. `publish_ipt.R`
stops if the new version has 0 records, or fewer than half the rows produced
by the transform in the same session.

## Digital Specimen DOIs

After GBIF harvests the dataset, DiSSCo assigns a Digital Specimen DOI to each
specimen (DataCite prefix `10.3535`). To surface them on GBIF they have to
come back into our data as `dwc:digitalSpecimenID`.

`scripts/fetch_dissco_dois.R` pulls the current `occurrenceID -> DOI` mapping
from the public DataCite API (no login) and writes `config/dissco_dois.csv`
(git-ignored, ~295k rows). `transform_to_dwc.R` joins that file on `id` and
fills `digitalSpecimenID`; if the file is absent the column is left empty.

```r
source("scripts/fetch_dissco_dois.R", echo = FALSE)   # takes a few minutes
```

Run it occasionally — a new specimen only gets a DOI after DiSSCo's next
harvest, so the mapping is always slightly behind. DiSSCo re-versions a
digital specimen when our data changes, but the DOI itself is stable, so no
DOI already stored ever needs updating.

## Outputs

- `data/raw/fm_raw_YYMMDD-HHMMSS.xlsx` — raw FileMaker export (only on `input_mode = "fetch"`)
- `data/dwc/occurrence_YYMMDD-HHMMSS.csv` — Darwin Core table
- `data/qc/qa_YYMMDD-HHMMSS.xlsx` — QA sheets, written only when there are coordinate or media issues

## Behaviour notes

- Coordinates are derived in order: decimal degrees → DMS → SWEREF99 TM → RT90, all output as EPSG:4326.
- Projected coordinates outside a plausible Swedish range are excluded and reported in the QA file.
- Duplicate `occurrenceID`s are reported to the console and flagged at the QA gate.
- Loading replaces all rows in `raw.fm_specimen` and `public.dwc_occurrence`.
- Every stage stops rather than pass an empty (0-row) dataset down the pipeline.
- The database connection is always closed, even if a load step fails.
