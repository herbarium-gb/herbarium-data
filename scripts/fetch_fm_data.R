# ------------------------------------------------------------
# FileMaker Data API -> fm_raw
# ------------------------------------------------------------
# - Fetches data via FileMaker Data API
# - Returns raw data as data.table
# - Writes raw export to Excel
# ------------------------------------------------------------

library(httr)
library(jsonlite)
library(data.table)
library(writexl)

# --- API config --------------------------------------------------------------

pwd <- Sys.getenv("HBDB_API_PWD")
if (!nzchar(pwd)) {
  stop("Missing env var: HBDB_API_PWD")
}

base_url <- Sys.getenv("FM_BASE_URL")
if (!nzchar(base_url)) {
  stop("Missing env var: FM_BASE_URL")
}

layout_name <- "GBIF_export"


# --- Function ----------------------------------------------------------------

fetch_filemaker_data <- function(pwd, batch_size = 1000, max_records = NULL) {
  
  sessions_url <- paste0(base_url, "/sessions")
  records_url <- paste0(base_url, "/layouts/", layout_name, "/records")
  
  
  # --- Login -----------------------------------------------------------------
  
  res_login <- POST(
    sessions_url,
    authenticate("api", pwd),
    body = "{}",
    encode = "raw",
    add_headers(`Content-Type` = "application/json"),
    timeout(120)
  )
  
  stop_for_status(res_login)
  
  token <- content(res_login, as = "parsed", type = "application/json")$response$token
  
  
  # --- Ensure logout on exit -------------------------------------------------
  
  on.exit(
    try(
      DELETE(
        paste0(sessions_url, "/", token),
        add_headers(Authorization = paste("Bearer", token)),
        timeout(30)
      ),
      silent = TRUE
    ),
    add = TRUE
  )
  
  
  # --- Initialize pagination state ------------------------------------------
  
  all_rows <- vector("list", 0)
  offset <- 1
  total_fetched <- 0
  found_count <- NA_integer_
  
  
  # --- Pagination loop -------------------------------------------------------
  
  repeat {
    current_limit <- if (is.null(max_records)) {
      batch_size
    } else {
      min(batch_size, max_records - total_fetched)
    }
    
    if (current_limit <= 0) break
    
    cat("Fetching offset =", offset, "limit =", current_limit, "\n")
    
    res <- GET(
      records_url,
      query = list(`_offset` = offset, `_limit` = current_limit),
      add_headers(Authorization = paste("Bearer", token)),
      timeout(120)
    )
    
    stop_for_status(res)
    
    out <- content(res, as = "parsed", type = "application/json")
    
    if (out$messages[[1]]$code != "0") {
      stop(out$messages[[1]]$message)
    }
    
    page_data <- out$response$data
    
    if (is.na(found_count)) {
      found_count <- as.integer(out$response$dataInfo$foundCount)
      cat("Total according to API:", found_count, "\n")
    }
    
    n <- length(page_data)
    cat("Returned:", n, "\n")
    
    if (n == 0) break
    
    rows <- lapply(page_data, function(x) {
      c(
        list(recordId = x$recordId, modId = x$modId),
        x$fieldData
      )
    })
    
    all_rows[[length(all_rows) + 1]] <- rows
    
    total_fetched <- total_fetched + n
    cat("Fetched so far:", total_fetched, "\n")
    
    if (!is.null(max_records) && total_fetched >= max_records) break
    if (n < current_limit) break
    if (!is.na(found_count) && total_fetched >= found_count) break
    
    offset <- offset + n
  }
  
  cat("Loop finished\n")
  
  if (length(all_rows) == 0) {
    return(data.table())
  }
  
  all_rows <- unlist(all_rows, recursive = FALSE)
  
  if (!is.null(max_records)) {
    all_rows <- all_rows[1:min(max_records, length(all_rows))]
  }
  
  cat("Binding rows...\n")
  
  fm_raw <- rbindlist(all_rows, fill = TRUE)
  
  fm_raw
}


# --- Fetch -------------------------------------------------------------------

# fm_raw <- fetch_filemaker_data(pwd, batch_size = 100, max_records = 500)
fm_raw <- fetch_filemaker_data(pwd, batch_size = 1000)

cat("Rows fetched:", nrow(fm_raw), "\n")


# --- Export ------------------------------------------------------------------

raw_file <- file.path(
  "data",
  "raw",
  paste0("fm_raw_", format(Sys.time(), "%y%m%d-%H%M%S"), ".xlsx")
)

write_xlsx(
  as.data.frame(fm_raw),
  raw_file
)

cat("Raw export written:", raw_file, "\n")

