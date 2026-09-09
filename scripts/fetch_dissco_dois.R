# ------------------------------------------------------------
# DiSSCo Digital Specimen DOIs  ->  config/dissco_dois.csv
# ------------------------------------------------------------
# DiSSCo mints a DOI per specimen after harvesting our GBIF
# dataset. This pulls the occurrenceID -> DOI mapping from the
# public DataCite API (no login) and writes a lookup CSV that
# transform_to_dwc.R joins into digitalSpecimenID.
#
# mode = "incremental" (default): only fetch DOIs changed since
#   the last run (config/dissco_dois_synced.txt) and merge into
#   the existing CSV. Falls back to a full fetch if either file
#   is missing.
# mode = "full": re-fetch everything and overwrite.
# ------------------------------------------------------------

library(httr)
library(jsonlite)

# --- Config ----------------------------------------------------------------

mode          <- "incremental"                 # "incremental" or "full"
org_ror       <- "https://ror.org/01tm6cn81"   # University of Gothenburg
source_system <- "477-1FN-0FH"                 # DiSSCo source system: Herbarium GB
doi_prefix    <- "10.3535"                     # DiSSCo's DataCite prefix
overlap_days  <- 2L                            # re-query a few days back, to be safe
page_size     <- 1000L
out_file      <- file.path("config", "dissco_dois.csv")
state_file    <- file.path("config", "dissco_dois_synced.txt")

# --- Decide mode ---------------------------------------------------------

have_prev <- file.exists(out_file) && file.exists(state_file)
effective <- if (identical(mode, "full") || !have_prev) "full" else "incremental"

query <- sprintf('creators.nameIdentifiers.nameIdentifier:"%s"', org_ror)

if (effective == "incremental") {
  since <- as.Date(trimws(readLines(state_file, warn = FALSE))[1]) - overlap_days
  query <- sprintf('%s AND updated:[%s TO *]', query, format(since, "%Y-%m-%d"))
  cat("Incremental fetch: DOIs updated since ", format(since), "\n", sep = "")
} else {
  cat("Full fetch of all Digital Specimen DOIs\n")
}

run_started <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

# --- Fetch ---------------------------------------------------------------

base <- "https://api.datacite.org/dois"

resp <- GET(base, query = list(
  prefix         = doi_prefix,
  query          = query,
  `fields[dois]` = "doi,identifiers",   # ~10x smaller pages; carried into next-links
  `page[size]`   = page_size,
  `page[cursor]` = 1
), timeout(120))
stop_for_status(resp)

page   <- fromJSON(content(resp, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
total  <- page$meta$total
cat("DOIs to page through: ", format(total, big.mark = " "), "\n", sep = "")

rows      <- list()
page_n    <- 0L
max_pages <- ceiling(max(total, 1) / page_size) + 5L

repeat {
  page_n <- page_n + 1L

  for (item in page$data) {
    a   <- item$attributes
    doi <- toupper(a$doi)   # DataCite lowercases; DiSSCo shows it uppercase

    psid <- NA_character_
    for (idn in a$identifiers) {
      if (identical(idn$identifierType, "primarySpecimenObjectId")) {
        psid <- idn$identifier
        break
      }
    }
    if (is.na(psid)) next

    parts <- strsplit(psid, ":", fixed = TRUE)[[1]]
    if (length(parts) < 2 || parts[2] != source_system) next

    rows[[length(rows) + 1L]] <- data.frame(
      id                = parts[1],
      digitalSpecimenID = paste0("https://doi.org/", doi),
      stringsAsFactors  = FALSE
    )
  }

  if (page_n %% 25L == 0L) cat("  page ", page_n, " - kept ", length(rows), "\n", sep = "")

  nxt <- page$links$`next`
  if (is.null(nxt) || page_n >= max_pages) break

  Sys.sleep(0.1)
  r <- GET(nxt, timeout(120))
  stop_for_status(r)
  page <- fromJSON(content(r, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
}

new <- if (length(rows) > 0) {
  d <- do.call(rbind, rows)
  d[!duplicated(d$id), ]
} else {
  data.frame(id = character(0), digitalSpecimenID = character(0), stringsAsFactors = FALSE)
}

# --- Merge + write -----------------------------------------------------

if (effective == "incremental" && file.exists(out_file)) {
  old    <- utils::read.csv(out_file, colClasses = "character")
  merged <- rbind(new, old[!(old$id %in% new$id), ])
} else {
  if (nrow(new) == 0) {
    stop("Full fetch returned no DOIs - check org_ror / source_system.")
  }
  merged <- new
}

merged <- merged[order(merged$id), ]

if (!dir.exists("config")) dir.create("config")
utils::write.csv(merged, out_file, row.names = FALSE, quote = FALSE)
writeLines(run_started, state_file)

cat("\n")
cat("--- Summary ----------------------------------------------------------\n")
cat("Mode:            ", effective,                        "\n", sep = "")
cat("Pages fetched:   ", page_n,                           "\n", sep = "")
cat("New / changed:   ", format(nrow(new),    big.mark = " "), "\n", sep = "")
cat("Total in lookup: ", format(nrow(merged), big.mark = " "), "\n", sep = "")
cat("Written:         ", out_file,                         "\n", sep = "")

rm(list = intersect(ls(), c(
  "mode", "org_ror", "source_system", "doi_prefix", "overlap_days", "page_size",
  "out_file", "state_file", "have_prev", "effective", "query", "since",
  "run_started", "base", "resp", "page", "total", "rows", "page_n", "max_pages",
  "nxt", "r", "item", "a", "doi", "psid", "idn", "parts", "d", "new", "old", "merged"
)))
