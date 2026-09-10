# ------------------------------------------------------------
# Run pipeline: FileMaker -> Darwin Core -> PostgreSQL -> IPT
# ------------------------------------------------------------
# - Chooses whether to read latest raw export file or fetch new data
# - Runs full transformation pipeline
# - Optionally loads latest raw and DwC files into PostgreSQL
# - Optionally publishes a new version of the IPT resource,
#   gated behind a QA check from transform_to_dwc.R
# ------------------------------------------------------------

rm(list = ls())

# --- Settings ----------------------------------------------------------------

input_mode   <- "file"   # "file" or "fetch"
target       <- "test"   # "test" or "prod" - which IPT to publish to
refresh_dois <- FALSE    # TRUE refreshes config/dissco_dois.csv before the transform
load_to_db   <- FALSE
check_media  <- FALSE
publish_ipt  <- FALSE

# --- Validate settings -------------------------------------------------------

if (!input_mode %in% c("file", "fetch")) {
  stop("Invalid input_mode. Use 'file' or 'fetch'.")
}
if (!target %in% c("test", "prod")) {
  stop("Invalid target. Use 'test' or 'prod'.")
}

# --- Resolve the IPT target ------------------------------------------------
# User and password differ between test and prod, so all four IPT vars are
# per-target in .Renviron: IPT_TEST_* and IPT_PROD_*.

local({
  pick <- function(v) Sys.getenv(sprintf("IPT_%s_%s", toupper(target), v))
  Sys.setenv(
    IPT_BASE_URL = pick("BASE_URL"),
    IPT_RESOURCE = pick("RESOURCE"),
    IPT_USER     = pick("USER"),
    IPT_PASS     = pick("PASS")
  )
})

# --- Run ---------------------------------------------------------------------

cat("Starting pipeline...\n\n")
cat("Input mode:         ", input_mode,  "\n", sep = "")
cat("IPT target:         ", target, " (", Sys.getenv("IPT_BASE_URL"), ")\n", sep = "")
cat("Refresh DOIs:       ", refresh_dois, "\n", sep = "")
cat("Load to PostgreSQL: ", load_to_db,  "\n", sep = "")
cat("Publish to IPT:     ", publish_ipt, "\n\n", sep = "")

if (input_mode == "fetch") {
  source("scripts/fetch_fm_data.R", echo = FALSE)

  if (!exists("fm_raw", inherits = FALSE)) {
    stop("scripts/fetch_fm_data.R did not create object 'fm_raw'.")
  }
}

if (refresh_dois) {
  source("scripts/fetch_dissco_dois.R", echo = FALSE)
}

source("scripts/transform_to_dwc.R", echo = FALSE)

if (load_to_db) {
  source("scripts/load_to_postgres.R", echo = FALSE)
}

if (publish_ipt) {
  source("scripts/publish_ipt.R", echo = FALSE)
}

cat("\nPipeline finished\n")
