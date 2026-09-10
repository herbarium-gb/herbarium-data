# herbarium-data

Pipeline for publishing the Gothenburg herbarium (GB) specimen collection to
[GBIF](https://www.gbif.org/). Records live in FileMaker; this repository
fetches them, transforms them to [Darwin Core](https://dwc.tdwg.org/), loads
the result into a PostgreSQL "publication layer", and triggers an
[IPT](https://www.gbif.org/ipt) to publish a new version. GBIF harvests the
IPT on its own schedule.

```
FileMaker  ──►  Darwin Core CSV  ──►  PostgreSQL  ──►  IPT version  ──►  GBIF
 (fetch)         (transform)          (load)           (publish)        (harvest)
```

The image server, web viewer, and the PostgreSQL/IPT deployment are managed
separately in **[herbarium-platform](https://github.com/herbarium-gb/herbarium-platform)**.

## Quick start

Assumes `.Renviron`, the SSH tunnel, and the IPT resource are already set up
(see below for first-time setup).

1. If running off-server, open the database tunnel in its own terminal:
   `ssh -N herbarium-db`
2. Open the project in RStudio and restart R, so `.Renviron` is loaded.
3. Set the switches at the top of `scripts/run_pipeline.R` (see
   [Running the pipeline](#running-the-pipeline)).
4. Run `scripts/run_pipeline.R` with the **Source** button.
5. Answer the prompts: `y` past any QA warnings, then `y` (test) or `PROD`
   (production) to publish.

Afterwards, check the new version and its record count on the IPT.

## Pipeline

`scripts/run_pipeline.R` runs the stages in order; each is also a standalone
script.

| Script | What it does |
|--------|--------------|
| `fetch_fm_data.R` | Pulls all records from the FileMaker Data API, writes `data/raw/fm_raw_*.xlsx`. Aborts if the API returns 0 rows. |
| `transform_to_dwc.R` | Maps source fields to Darwin Core via `config/col-map.xlsx`, derives coordinates and `eventDate`, joins Digital Specimen DOIs if `config/dissco_dois.csv` is present, writes `data/dwc/occurrence_*.csv` and (on issues) `data/qc/qa_*.xlsx`. |
| `load_to_postgres.R` | Replaces `raw.fm_specimen` and `public.dwc_occurrence` with the latest raw and DwC files. Refuses to load an empty file. |
| `publish_ipt.R` | Logs in to the IPT, publishes a new resource version, polls until it finishes, checks the record count. |

`scripts/fetch_dissco_dois.R` is a separate occasional job — see
[Digital Specimen DOIs](#digital-specimen-dois).

## Requirements

R (4.x) with:

```r
install.packages(c(
  "httr", "jsonlite", "data.table", "readxl",
  "sf", "readr", "writexl", "DBI", "RPostgres"
))
```

## Configuration

Copy `.Renviron.template` to `.Renviron` in the project root and fill in the
values (FileMaker, PostgreSQL, and per-target `IPT_TEST_*` / `IPT_PROD_*`).

Restart R after editing `.Renviron` — it is only read at startup. The file
holds passwords: it is git-ignored; also run `chmod 600 .Renviron`.
`HBDB_API_USR` defaults to `api` if unset.

## Running the pipeline

Run `scripts/run_pipeline.R` with RStudio's **Source** button, not by
stepping through the lines — otherwise the confirmation prompts consume the
following lines as their answers.

Switches at the top of the script:

```r
input_mode  <- "file"   # "file" = latest data/raw file; "fetch" = pull from FileMaker
target      <- "test"   # "test" or "prod" - which IPT to publish to
load_to_db  <- FALSE    # TRUE writes to PostgreSQL (TRUNCATE + reload both tables)
check_media <- FALSE    # TRUE opens every associatedMedia URL to check it (slow)
publish_ipt <- FALSE    # TRUE publishes a new IPT version
```

`target` selects which `IPT_TEST_*` / `IPT_PROD_*` block from `.Renviron` is
used.

Prompts:

- **QA gate** — duplicate `occurrenceID`s, invalid projected coordinates,
  broken media links, and unbalanced quotes are listed; you choose whether to
  continue.
- **Publish confirmation** — `test` asks for a plain `y`; any IPT whose URL
  does not contain `test` is treated as production and asks you to type
  `PROD` (exactly, uppercase).

## FileMaker Data API

The fetch stage needs three things set up once on the FileMaker side:

1. **Data API enabled** on FileMaker Server (Admin Console → Connectors).
2. **An API account** on the hosted file with the `fmrest` extended
   privilege. `fetch_fm_data.R` logs in as `HBDB_API_USR` (default `api`)
   with `HBDB_API_PWD`.
3. **A layout** named `GBIF_export`, visible to that account, exposing the
   source fields the transform expects.

An error at `POST /sessions` points to the account, its `fmrest` privilege,
or the server toggle; an error once logged in points to the `GBIF_export`
layout.

## PostgreSQL access

The pipeline and the IPT read the same PostgreSQL database — a staging layer
whose contents are replaced on every load. It must already contain the tables
`raw.fm_specimen` and `public.dwc_occurrence`.

On the database server itself, set `PGPORT=5432`, no tunnel.

From your own machine, reach it over an SSH tunnel that forwards local `5433`
to `5432` on the server (`5433` avoids clashing with a local PostgreSQL).
Add a `~/.ssh/config` entry (ask the maintainer for host and user):

```
Host herbarium-db            # any name; used in ssh -N below
  HostName <database server>
  User <your user>
  IdentityFile ~/.ssh/<your key>
  LocalForward 5433 localhost:5432
```

then run `ssh -N herbarium-db` in its own terminal and set `PGHOST=localhost`
/ `PGPORT=5433` in `.Renviron`. (A one-off `ssh -N -L 5433:localhost:5432
user@your-server` works too.)

## IPT publishing

`publish_ipt.R` mimics the IPT web UI (there is no REST API): fetch a CSRF
token, log in with a form + session cookie, POST `manage/publish.do`, poll
`manage/report.do` until done.

It does **not** configure the resource. Before it can publish, the IPT
resource must already have — set up by hand in the IPT — a source (the
PostgreSQL connection), a complete Darwin Core Occurrence mapping, a
publishing organisation, and the mandatory metadata. A resource missing any
of these publishes an empty archive; `publish_ipt.R` stops if the new version
has 0 records, or fewer than half the rows from the transform in the same
session.

## Digital Specimen DOIs

After GBIF harvests the dataset, DiSSCo assigns a Digital Specimen DOI to each
specimen (DataCite prefix `10.3535`). To surface them on GBIF they must come
back into the data as `dwc:digitalSpecimenID`.

`scripts/fetch_dissco_dois.R` pulls the `occurrenceID -> DOI` mapping from the
public DataCite API (no login) into `config/dissco_dois.csv` (git-ignored);
`transform_to_dwc.R` joins it on `id`. It runs **incrementally** — after the
first full run (a few minutes, ~295k) it only fetches DOIs changed since the
timestamp in `config/dissco_dois_synced.txt`, so later runs take seconds. Set
`mode <- "full"` in the script to force a complete re-fetch.

Run it occasionally: a new specimen gets a DOI only after DiSSCo's next
harvest, and a DOI never changes once assigned.

## Outputs

- `data/raw/fm_raw_*.xlsx` — raw FileMaker export (only on `input_mode = "fetch"`)
- `data/dwc/occurrence_*.csv` — Darwin Core table
- `data/qc/qa_*.xlsx` — QA sheets, only when there are issues

## Behaviour notes

- Coordinates are derived in order decimal → DMS → SWEREF99 TM → RT90, all output as EPSG:4326.
- Projected coordinates outside a plausible Swedish range are excluded and reported.
- Loading replaces all rows in the target tables.
- Every stage stops rather than pass an empty dataset down the pipeline.
- The database connection is always closed, even if a load step fails.
