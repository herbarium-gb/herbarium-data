# ------------------------------------------------------------
# Herbarium export files -> PostgreSQL
# ------------------------------------------------------------
# - Uses latest raw export file by default:
#   * data/raw/fm_raw_*.xlsx / .xls / .csv
# - Uses latest DwC export file by default:
#   * data/dwc/occurrence_*.csv
# - Connects to PostgreSQL using environment variables
# - Replaces table contents on each run:
#   * TRUNCATE raw.fm_specimen
#   * TRUNCATE public.dwc_occurrence
# - Loads both files with RPostgres COPY support
# - Reports final row counts for both tables
# ------------------------------------------------------------

library(DBI)
library(RPostgres)
library(readr)
library(readxl)

# --- Helpers -----------------------------------------------------------------

get_env <- function(name) {
  value <- Sys.getenv(name)
  
  if (!nzchar(value)) {
    stop(sprintf("Missing env var: %s", name))
  }
  
  value
}

get_latest_file <- function(path, pattern) {
  files <- list.files(path, pattern = pattern, full.names = TRUE)
  
  if (length(files) == 0) {
    stop(sprintf("No files found in '%s' matching '%s'", path, pattern))
  }
  
  files[which.max(file.info(files)$mtime)]
}


# --- Input files -------------------------------------------------------------

raw_file <- get_latest_file(
  file.path("data", "raw"),
  "^fm_raw_.*\\.(xlsx|xls|csv)$"
)

dwc_file <- get_latest_file(
  file.path("data", "dwc"),
  "^occurrence_.*\\.csv$"
)

cat("Using raw file: ", raw_file, "\n", sep = "")
cat("Using DwC file: ", dwc_file, "\n", sep = "")


# --- Read input --------------------------------------------------------------

raw_df <- if (grepl("\\.csv$", raw_file, ignore.case = TRUE)) {
  readr::read_csv(
    raw_file,
    show_col_types = FALSE,
    col_types = readr::cols(.default = readr::col_character())
  )
} else {
  as.data.frame(
    readxl::read_excel(raw_file, col_types = "text"),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
}

dwc_df <- readr::read_csv(
  dwc_file,
  show_col_types = FALSE,
  col_types = readr::cols(.default = readr::col_character())
)


# --- Database connection -----------------------------------------------------

con <- dbConnect(
  RPostgres::Postgres(),
  dbname = get_env("PGDATABASE"),
  host = Sys.getenv("PGHOST", "localhost"),
  port = as.integer(Sys.getenv("PGPORT", "5432")),
  user = get_env("PGUSER"),
  password = get_env("PGPASSWORD")
)

if (!DBI::dbIsValid(con)) {
  stop("PostgreSQL connection is not valid.")
}

cat("PostgreSQL connection: OK\n")


# --- Replace raw and DwC tables ----------------------------------------------

dbWithTransaction(con, {
  dbExecute(con, 'TRUNCATE TABLE raw.fm_specimen')
  
  dbWriteTable(
    con,
    Id(schema = "raw", table = "fm_specimen"),
    raw_df,
    append = TRUE,
    copy = TRUE,
    row.names = FALSE
  )
  
  dbExecute(con, 'TRUNCATE TABLE public.dwc_occurrence')
  
  dbWriteTable(
    con,
    Id(schema = "public", table = "dwc_occurrence"),
    dwc_df,
    append = TRUE,
    copy = TRUE,
    row.names = FALSE
  )
})


# --- Summary -----------------------------------------------------------------

raw_n <- dbGetQuery(
  con,
  'SELECT count(*) AS n FROM raw.fm_specimen'
)$n[[1]]

dwc_n <- dbGetQuery(
  con,
  'SELECT count(*) AS n FROM public.dwc_occurrence'
)$n[[1]]

cat("\n")
cat("--- Summary ----------------------------------------------------------\n")
cat("Raw file: ", raw_file, "\n", sep = "")
cat("DwC file: ", dwc_file, "\n", sep = "")
cat("Rows in raw.fm_specimen: ", format(raw_n, big.mark = " "), "\n", sep = "")
cat("Rows in public.dwc_occurrence: ", format(dwc_n, big.mark = " "), "\n", sep = "")
cat("Load complete.\n")


# --- Disconnect --------------------------------------------------------------

DBI::dbDisconnect(con)