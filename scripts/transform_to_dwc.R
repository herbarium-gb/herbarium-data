# ------------------------------------------------------------
# Herbarium export -> Darwin Core (DwC)
# ------------------------------------------------------------
# - Loads raw herbarium export data and column mapping
# - Uses fresh API data or latest raw export file
# - Maps source fields to Darwin Core fields
# - Standardizes eventDate from Year / Month / Day
# - Derives decimal coordinates from:
#   * direct decimal degrees
#   * DMS coordinates
#   * SWEREF99 TM
#   * RT90
# - Excludes invalid projected coordinates from transformation
# - Checks associatedMedia links
# - Exports DwC output and QA tables
# ------------------------------------------------------------

library(readxl)
library(data.table)
library(sf)
library(readr)
library(writexl)

# --- Input -------------------------------------------------------------------

input_source <- NULL

if (exists("fm_raw", inherits = FALSE)) {
  input_source <- "API fetch (current session)"
} else {
  files <- list.files(
    file.path("data", "raw"),
    pattern = "^fm_raw_\\d{6}-\\d{6}\\.xlsx$",
    full.names = TRUE
  )
  
  if (length(files) == 0) {
    stop("No fm_raw file found. Run scripts/run_pipeline.R with input_mode = 'fetch' first.")
  }
  
  latest_file <- max(files)
  fm_raw <- as.data.table(read_excel(latest_file, col_types = "text"))
  input_source <- paste("file:", latest_file)
}

col_map <- as.data.table(
  read_excel(file.path("config", "col-map.xlsx"), col_types = "text")
)

# --- Helpers -----------------------------------------------------------------

to_num <- function(x) {
  suppressWarnings(as.numeric(gsub(",", ".", trimws(as.character(x)))))
}

has_value <- function(x) {
  !is.na(x) & trimws(as.character(x)) != ""
}

append_json_prop <- function(old, key, value) {
  old <- as.character(old)
  value <- as.character(value)
  out <- old
  
  esc <- function(x) {
    x <- gsub("\\\\", "\\\\\\\\", x)
    gsub("\"", "\\\\\"", x)
  }
  
  idx <- has_value(value)
  new_json <- paste0("{\"", key, "\":\"", esc(value), "\"}")
  
  out[idx & !has_value(old)] <- new_json[idx & !has_value(old)]
  out[idx & has_value(old)] <- paste0(
    sub("\\}$", "", old[idx & has_value(old)]),
    ",\"",
    key,
    "\":\"",
    esc(value[idx & has_value(old)]),
    "\"}"
  )
  
  out
}

dms_to_decimal <- function(deg, min, sec, dir) {
  deg <- to_num(deg)
  min <- to_num(min)
  sec <- to_num(sec)
  
  min[is.na(min)] <- 0
  sec[is.na(sec)] <- 0
  
  dec <- deg + min / 60 + sec / 3600
  
  dir <- toupper(trimws(as.character(dir)))
  dec[dir %in% c("W", "S")] <- -dec[dir %in% c("W", "S")]
  
  dec
}

is_valid_rt90 <- function(n, o) {
  !is.na(n) & !is.na(o) &
    n >= 6100000 & n <= 7700000 &
    o >= 1200000 & o <= 1900000
}

is_valid_sweref <- function(n, o) {
  !is.na(n) & !is.na(o) &
    n >= 6100000 & n <= 7700000 &
    o >= 250000 & o <= 950000
}

check_media_exists <- function(urls) {
  urls <- trimws(as.character(urls))
  
  vapply(urls, function(u) {
    if (is.na(u) || u == "") return(NA)
    
    tryCatch({
      con <- url(u, open = "rb")
      close(con)
      TRUE
    }, error = function(e) {
      FALSE
    })
  }, logical(1))
}

# --- Prepare metadata --------------------------------------------------------

setnames(col_map, trimws(names(col_map)))

dwc_cols <- trimws(col_map$`dwc-field`)
dwc_cols <- dwc_cols[dwc_cols != "" & !is.na(dwc_cols)]

derived_cols <- c(
  "decimalLatitude",
  "decimalLongitude",
  "geodeticDatum",
  "georeferenceRemarks",
  "verbatimLatitude",
  "verbatimLongitude",
  "verbatimCoordinateSystem",
  "verbatimSRS",
  "dynamicProperties",
  "eventDate",
  "year",
  "month",
  "day"
)

all_cols <- unique(c(dwc_cols, derived_cols))

# --- Initialize DwC table ----------------------------------------------------

dwc <- data.table(matrix(
  NA_character_,
  nrow = nrow(fm_raw),
  ncol = length(all_cols)
))

setnames(dwc, all_cols)

# --- Map source fields -------------------------------------------------------

if ("source-field" %in% names(col_map)) {
  for (i in seq_len(nrow(col_map))) {
    dst <- trimws(col_map$`dwc-field`[i])
    src <- trimws(col_map$`source-field`[i])
    
    if (dst %in% names(dwc) && src %in% names(fm_raw) && src != "") {
      dwc[[dst]] <- as.character(fm_raw[[src]])
    }
  }
}

# --- Apply constant values ---------------------------------------------------

if ("value" %in% names(col_map)) {
  for (i in seq_len(nrow(col_map))) {
    dst <- trimws(col_map$`dwc-field`[i])
    val <- col_map$value[i]
    
    if (dst %in% names(dwc) && has_value(val)) {
      dwc[[dst]] <- as.character(val)
    }
  }
}

# --- Derived identifiers and media -------------------------------------------

if (!"id" %in% names(dwc) && "AccessionNo" %in% names(fm_raw)) {
  set(dwc, j = "id", value = fm_raw$AccessionNo)
}

if ("Image1" %in% names(fm_raw) && "AccessionNo" %in% names(fm_raw)) {
  idx <- has_value(fm_raw$Image1)
  
  dwc[idx, associatedMedia := paste0(
    "https://herbarium.gu.se/web/images/",
    fm_raw$AccessionNo[idx],
    ".jpg"
  )]
}

# --- Prepare numeric coordinate fields ---------------------------------------

fm_raw[, `:=`(
  Sweref_N_num = to_num(Sweref_N),
  Sweref_O_num = to_num(Sweref_O),
  RiketsN_num  = to_num(RiketsN),
  RiketsO_num  = to_num(RiketsO)
)]

# --- QA tables for invalid projected coordinates -----------------------------

bad_sweref <- fm_raw[
  has_value(Sweref_N) & has_value(Sweref_O) &
    !is_valid_sweref(Sweref_N_num, Sweref_O_num),
  .(
    AccessionNo,
    Sweref_N,
    Sweref_O,
    LatitudeDegree,
    LatitudeMinute,
    LatitudeSecond,
    LatitudeDirection,
    LongitudeDegree,
    LongitudeMinute,
    LongitudeSecond,
    LongitudeDirection,
    reason = "SWEREF outside plausible Swedish range"
  )
]

bad_rt90 <- fm_raw[
  has_value(RiketsN) & has_value(RiketsO) &
    !is_valid_rt90(RiketsN_num, RiketsO_num),
  .(
    AccessionNo,
    RiketsN,
    RiketsO,
    LatitudeDegree,
    LatitudeMinute,
    LatitudeSecond,
    LatitudeDirection,
    LongitudeDegree,
    LongitudeMinute,
    LongitudeSecond,
    LongitudeDirection,
    reason = "RT90 outside plausible Swedish range"
  )
]

# --- Coordinates: 1. direct decimal degrees ----------------------------------

lon_deg <- to_num(fm_raw$LongitudeDegree)
lat_deg <- to_num(fm_raw$LatitudeDegree)

idx_direct <- has_value(fm_raw$LongitudeDegree) &
  has_value(fm_raw$LatitudeDegree) &
  grepl("\\.", fm_raw$LongitudeDegree) &
  grepl("\\.", fm_raw$LatitudeDegree)

dwc[idx_direct, `:=`(
  decimalLongitude = sprintf("%.5f", lon_deg[idx_direct]),
  decimalLatitude = sprintf("%.5f", lat_deg[idx_direct]),
  geodeticDatum = "EPSG:4326",
  georeferenceRemarks = NA_character_,
  verbatimLatitude = trimws(fm_raw$LatitudeDegree[idx_direct]),
  verbatimLongitude = trimws(fm_raw$LongitudeDegree[idx_direct]),
  verbatimCoordinateSystem = "decimal degrees",
  verbatimSRS = "EPSG:4326"
)]

# --- Coordinates: 2. DMS -----------------------------------------------------

idx_missing <- !(has_value(dwc$decimalLatitude) & has_value(dwc$decimalLongitude))

idx_dms <- idx_missing &
  has_value(fm_raw$LongitudeDegree) &
  has_value(fm_raw$LatitudeDegree) &
  !grepl("\\.", fm_raw$LongitudeDegree) &
  !grepl("\\.", fm_raw$LatitudeDegree)

lon_num <- dms_to_decimal(
  fm_raw$LongitudeDegree,
  fm_raw$LongitudeMinute,
  fm_raw$LongitudeSecond,
  fm_raw$LongitudeDirection
)

lat_num <- dms_to_decimal(
  fm_raw$LatitudeDegree,
  fm_raw$LatitudeMinute,
  fm_raw$LatitudeSecond,
  fm_raw$LatitudeDirection
)

lon_txt <- sprintf("%.4f", lon_num)
lat_txt <- sprintf("%.4f", lat_num)

lon_txt[idx_dms & has_value(fm_raw$LongitudeSecond)] <- sprintf(
  "%.5f",
  lon_num[idx_dms & has_value(fm_raw$LongitudeSecond)]
)

lat_txt[idx_dms & has_value(fm_raw$LatitudeSecond)] <- sprintf(
  "%.5f",
  lat_num[idx_dms & has_value(fm_raw$LatitudeSecond)]
)

ver_lat_dms <- gsub(
  "\\s+",
  " ",
  trimws(paste(
    fm_raw$LatitudeDegree,
    fifelse(has_value(fm_raw$LatitudeMinute), fm_raw$LatitudeMinute, ""),
    fifelse(has_value(fm_raw$LatitudeSecond), fm_raw$LatitudeSecond, ""),
    fm_raw$LatitudeDirection
  ))
)

ver_lon_dms <- gsub(
  "\\s+",
  " ",
  trimws(paste(
    fm_raw$LongitudeDegree,
    fifelse(has_value(fm_raw$LongitudeMinute), fm_raw$LongitudeMinute, ""),
    fifelse(has_value(fm_raw$LongitudeSecond), fm_raw$LongitudeSecond, ""),
    fm_raw$LongitudeDirection
  ))
)

dwc[idx_dms, `:=`(
  decimalLongitude = lon_txt[idx_dms],
  decimalLatitude = lat_txt[idx_dms],
  geodeticDatum = "EPSG:4326",
  georeferenceRemarks = NA_character_,
  verbatimLatitude = ver_lat_dms[idx_dms],
  verbatimLongitude = ver_lon_dms[idx_dms],
  verbatimCoordinateSystem = "DMS",
  verbatimSRS = "EPSG:4326"
)]

# --- Coordinates: 3. SWEREF99 TM ---------------------------------------------

idx_missing <- !(has_value(dwc$decimalLatitude) & has_value(dwc$decimalLongitude))

idx_sweref <- idx_missing &
  has_value(fm_raw$Sweref_O) &
  has_value(fm_raw$Sweref_N) &
  is_valid_sweref(fm_raw$Sweref_N_num, fm_raw$Sweref_O_num)

if (any(idx_sweref)) {
  pts <- st_as_sf(
    fm_raw[idx_sweref, .(x = Sweref_O_num, y = Sweref_N_num)],
    coords = c("x", "y"),
    crs = 3006
  )
  
  pts <- st_transform(pts, 4326)
  coords <- st_coordinates(pts)
  
  dwc[idx_sweref, `:=`(
    decimalLongitude = sprintf("%.5f", coords[, 1]),
    decimalLatitude = sprintf("%.5f", coords[, 2]),
    geodeticDatum = "EPSG:4326",
    georeferenceRemarks = NA_character_,
    verbatimLatitude = trimws(fm_raw$Sweref_N[idx_sweref]),
    verbatimLongitude = trimws(fm_raw$Sweref_O[idx_sweref]),
    verbatimCoordinateSystem = "SWEREF99 TM",
    verbatimSRS = "EPSG:3006"
  )]
}

# --- Coordinates: 4. RT90 ----------------------------------------------------

idx_missing <- !(has_value(dwc$decimalLatitude) & has_value(dwc$decimalLongitude))

idx_rt90 <- idx_missing &
  has_value(fm_raw$RiketsO) &
  has_value(fm_raw$RiketsN) &
  is_valid_rt90(fm_raw$RiketsN_num, fm_raw$RiketsO_num)

if (any(idx_rt90)) {
  pts <- st_as_sf(
    fm_raw[idx_rt90, .(x = RiketsO_num, y = RiketsN_num)],
    coords = c("x", "y"),
    crs = 3021
  )
  
  pts <- st_transform(pts, 4326)
  coords <- st_coordinates(pts)
  
  dwc[idx_rt90, `:=`(
    decimalLongitude = sprintf("%.5f", coords[, 1]),
    decimalLatitude = sprintf("%.5f", coords[, 2]),
    geodeticDatum = "EPSG:4326",
    georeferenceRemarks = NA_character_,
    verbatimLatitude = trimws(fm_raw$RiketsN[idx_rt90]),
    verbatimLongitude = trimws(fm_raw$RiketsO[idx_rt90]),
    verbatimCoordinateSystem = "RT90",
    verbatimSRS = "EPSG:3021"
  )]
}

# --- Mark excluded projected coordinates -------------------------------------

bad_proj_any <- unique(c(bad_sweref$AccessionNo, bad_rt90$AccessionNo))

if (length(bad_proj_any) > 0 && "AccessionNo" %in% names(fm_raw)) {
  idx_bad <- fm_raw$AccessionNo %in% bad_proj_any
  
  dwc[
    idx_bad & !has_value(dwc$georeferenceRemarks),
    georeferenceRemarks := "Projected coordinates outside plausible Swedish range; excluded from transformation"
  ]
}

# --- Dynamic properties ------------------------------------------------------

if ("RUBIN" %in% names(fm_raw)) {
  dwc[, dynamicProperties := append_json_prop(dynamicProperties, "RUBIN", fm_raw$RUBIN)]
}

# --- eventDate ---------------------------------------------------------------

yr <- suppressWarnings(as.integer(trimws(as.character(fm_raw$Year))))
mo <- suppressWarnings(as.integer(trimws(as.character(fm_raw$Month))))
dy <- suppressWarnings(as.integer(trimws(as.character(fm_raw$Day))))

date <- rep(NA_character_, nrow(fm_raw))

has_year  <- !is.na(yr)
has_month <- has_year & !is.na(mo)
has_day   <- has_month & !is.na(dy)

date[has_year]  <- sprintf("%04d", yr[has_year])
date[has_month] <- sprintf("%04d-%02d", yr[has_month], mo[has_month])
date[has_day]   <- sprintf("%04d-%02d-%02d", yr[has_day], mo[has_day], dy[has_day])

dwc[, eventDate := date]
dwc[, year := yr]
dwc[, month := mo]
dwc[, day := dy]

# --- Clean character fields --------------------------------------------------

dwc[] <- lapply(dwc, function(x) {
  if (is.character(x)) {
    gsub("[[:cntrl:]]+", " ", x)
  } else {
    x
  }
})

# --- Reorder columns ---------------------------------------------------------

wanted_order <- c(
  "id",
  "institutionCode",
  "collectionCode",
  "basisOfRecord",
  "occurrenceID",
  "occurrenceStatus",
  "catalogNumber",
  "recordNumber",
  "recordedBy",
  "associatedMedia",
  "occurrenceRemarks",
  "verbatimLabel",
  "eventDate",
  "year",
  "month",
  "day",
  "continent",
  "country",
  "stateProvince",
  "county",
  "locality",
  "decimalLatitude",
  "decimalLongitude",
  "geodeticDatum",
  "georeferenceRemarks",
  "verbatimLatitude",
  "verbatimLongitude",
  "verbatimCoordinateSystem",
  "verbatimSRS",
  "dynamicProperties",
  "scientificName",
  "scientificNameAuthorship",
  "originalNameUsage",
  "verbatimIdentification",
  "identificationRemarks",
  "genus",
  "specificEpithet",
  "infraspecificEpithet",
  "taxonRemarks",
  "typeStatus",
  "minimumElevationInMeters",
  "maximumElevationInMeters"
)

wanted_order <- wanted_order[wanted_order %in% names(dwc)]
extra_cols <- setdiff(names(dwc), wanted_order)

dwc <- dwc[, c(wanted_order, extra_cols), with = FALSE]

# --- QA summary objects ------------------------------------------------------

qa_sheets <- list()

n_rows <- nrow(dwc)

dup_ids <- data.table()
n_dup_ids <- 0L
n_dup_rows <- 0L

if ("id" %in% names(dwc)) {
  dup_ids <- dwc[, .N, by = id][N > 1][order(-N, id)]
  n_dup_ids <- nrow(dup_ids)
  n_dup_rows <- if (n_dup_ids > 0) sum(dup_ids$N) else 0L
}

n_bad_sweref <- nrow(bad_sweref)
n_bad_rt90 <- nrow(bad_rt90)

if (n_bad_rt90 > 0) {
  qa_sheets$bad_rt90 <- bad_rt90
}

if (n_bad_sweref > 0) {
  qa_sheets$bad_sweref <- bad_sweref
}

# --- QA: media links ---------------------------------------------------------

bad_media <- data.table()
n_bad_media <- NA_integer_

if (check_media && "associatedMedia" %in% names(dwc)) {
  media_ok <- check_media_exists(dwc$associatedMedia)
  
  bad_media <- dwc[!is.na(media_ok) & !media_ok, .(
    id,
    associatedMedia
  )]
  
  n_bad_media <- nrow(bad_media)
  
  if (n_bad_media > 0) {
    qa_sheets$bad_media <- bad_media
  }
}

# --- Export ------------------------------------------------------------------

out_file <- file.path(
  "data",
  "dwc",
  paste0("occurrence_", format(Sys.time(), "%y%m%d-%H%M%S"), ".csv")
)

readr::write_excel_csv(
  dwc,
  out_file,
  na = ""
)

qa_file <- file.path(
  "data",
  "qc",
  paste0("qa_", format(Sys.time(), "%y%m%d-%H%M%S"), ".xlsx")
)

if (length(qa_sheets) > 0) {
  write_xlsx(qa_sheets, qa_file)
}

# --- Summary -----------------------------------------------------------------

cat("\n")
cat("--- Summary ----------------------------------------------------------\n")
cat("Input source: ", input_source, "\n", sep = "")
cat("Output file: ", out_file, "\n", sep = "")
cat("Rows in dwc: ", format(n_rows, big.mark = " "), "\n", sep = "")
cat("Invalid SWEREF rows: ", format(n_bad_sweref, big.mark = " "), "\n", sep = "")
cat("Invalid RT90 rows: ", format(n_bad_rt90, big.mark = " "), "\n", sep = "")
if (is.na(n_bad_media)) {
  cat("Invalid media links: not checked\n")
} else {
  cat("Invalid media links: ", format(n_bad_media, big.mark = " "), "\n", sep = "")
}

if (length(qa_sheets) > 0) {
  cat("QA file written: ", qa_file, "\n", sep = "")
} else {
  cat("QA file not written: no QA issues found\n")
}

if (!"id" %in% names(dwc)) {
  cat("ID check: skipped ('id' column not found)\n")
} else if (n_dup_ids == 0) {
  cat("ID check: OK (no duplicate ids)\n")
} else {
  cat("ID check: DUPLICATES FOUND\n")
  cat("Duplicated ids: ", format(n_dup_ids, big.mark = " "), "\n", sep = "")
  cat("Rows involved: ", format(n_dup_rows, big.mark = " "), "\n", sep = "")
  print(dup_ids)
}

# --- Keep only main outputs --------------------------------------------------

rm(list = setdiff(ls(), c(
  "fm_raw",
  "dwc",
  "bad_rt90",
  "bad_sweref",
  "bad_media",
  "dup_ids",
  "load_to_db",
  "input_mode"
)))