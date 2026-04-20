# ------------------------------------------------------------
# Run pipeline: FileMaker -> Darwin Core -> PostgreSQL
# ------------------------------------------------------------
# - Chooses whether to read latest raw export file or fetch new data
# - Runs full transformation pipeline
# - Optionally loads latest raw and DwC files into PostgreSQL
# ------------------------------------------------------------

rm(list = ls())

# --- Settings ----------------------------------------------------------------

# input_mode <- "file"   # "file" or "fetch"
input_mode <- "file"

load_to_db <- TRUE
check_media <- FALSE


# --- Validate settings -------------------------------------------------------

if (!input_mode %in% c("file", "fetch")) {
  stop("Invalid input_mode. Use 'file' or 'fetch'.")
}


# --- Run ---------------------------------------------------------------------

cat("Starting pipeline...\n\n")
cat("Input mode: ", input_mode, "\n", sep = "")
cat("Load to PostgreSQL: ", load_to_db, "\n\n", sep = "")

if (input_mode == "fetch") {
  source("scripts/fetch_fm_data.R", echo = FALSE)
  
  if (!exists("fm_raw", inherits = FALSE)) {
    stop("scripts/fetch_fm_data.R did not create object 'fm_raw'.")
  }
}

source("scripts/transform_to_dwc.R", echo = FALSE)

if (load_to_db) {
  source("scripts/load_to_postgres.R", echo = FALSE)
}

cat("\nPipeline finished\n")