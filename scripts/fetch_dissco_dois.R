# ------------------------------------------------------------
# DiSSCo Digital Specimen DOIs  ->  config/dissco_dois.csv
# ------------------------------------------------------------
# DiSSCo mints a DOI per specimen after harvesting our GBIF
# dataset. This pulls the current occurrenceID -> DOI mapping
# from the public DataCite API (no login) and writes a lookup
# CSV that transform_to_dwc.R joins into digitalSpecimenID.
#
# Run it now and then - new specimens get a DOI only after
# DiSSCo's next harvest, so it is always slightly behind.
# ------------------------------------------------------------

library(httr)
library(jsonlite)

# --- Config ----------------------------------------------------------------

org_ror       <- "https://ror.org/01tm6cn81"   # University of Gothenburg
source_system <- "477-1FN-0FH"                  # DiSSCo source system: Herbarium GB
doi_prefix    <- "10.3535"                      # DiSSCo's DataCite prefix
page_size     <- 1000L
out_file      <- file.path("config", "dissco_dois.csv")

# --- Fetch ---------------------------------------------------------------

base   <- "https://api.datacite.org/dois"
query  <- sprintf('creators.nameIdentifiers.nameIdentifier:"%s"', org_ror)

resp <- GET(base, query = list(
  prefix         = doi_prefix,
  query          = query,
  `page[size]`   = page_size,
  `page[cursor]` = 1
), timeout(120))
stop_for_status(resp)

first <- fromJSON(content(resp, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
total <- first$meta$total
cat("DOIs to page through: ", format(total, big.mark = " "), "\n", sep = "")

rows      <- list()
page      <- first
page_n    <- 0L
max_pages <- ceiling(total / page_size) + 5L

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
    if (length(parts) < 2 || parts[2] != source_system) next   # wrong / other collection

    rows[[length(rows) + 1L]] <- data.frame(
      id                = parts[1],
      digitalSpecimenID = paste0("https://doi.org/", doi),
      stringsAsFactors  = FALSE
    )
  }

  if (page_n %% 25L == 0L || is.null(page$links$`next`)) {
    cat("  page ", page_n, " - kept ", length(rows), " so far\n", sep = "")
  }

  nxt <- page$links$`next`
  if (is.null(nxt) || page_n >= max_pages) break

  Sys.sleep(0.1)
  r <- GET(nxt, timeout(120))
  stop_for_status(r)
  page <- fromJSON(content(r, as = "text", encoding = "UTF-8"), simplifyVector = FALSE)
}

# --- Write -------------------------------------------------------------

if (length(rows) == 0) {
  stop("No matching DOIs returned - check org_ror / source_system, or DiSSCo has not harvested yet.")
}

dois <- do.call(rbind, rows)
dois <- dois[!duplicated(dois$id), ]

if (!dir.exists("config")) dir.create("config")
utils::write.csv(dois, out_file, row.names = FALSE, quote = FALSE)

cat("\n")
cat("--- Summary ----------------------------------------------------------\n")
cat("Pages fetched:   ", page_n,       "\n", sep = "")
cat("occurrenceIDs:   ", format(nrow(dois), big.mark = " "), "\n", sep = "")
cat("Written:         ", out_file,     "\n", sep = "")

rm(base, query, resp, first, total, rows, page, page_n, max_pages, nxt, r, dois,
   org_ror, source_system, doi_prefix, page_size, out_file)
