# ------------------------------------------------------------
# IPT publish: QA gate -> publish new resource version
# ------------------------------------------------------------
# Reads QA objects from transform_to_dwc.R, warns interactively
# on issues, then publishes a new version of the IPT resource.
#
# The IPT has no REST API for publishing, so this mimics the web
# UI: obtain a CSRF token, log in (form + session cookie), POST
# manage/publish.do, then poll manage/report.do until it finishes.
#
# The body runs inside a function so early exits (return()) work
# when the file is source()d from run_pipeline.R.
# ------------------------------------------------------------

library(httr)

# --- Credentials (a missing var should stop the whole pipeline) -------------

ipt_base_url <- Sys.getenv("IPT_BASE_URL"); if (!nzchar(ipt_base_url)) stop("Missing env var: IPT_BASE_URL")
ipt_resource <- Sys.getenv("IPT_RESOURCE"); if (!nzchar(ipt_resource)) stop("Missing env var: IPT_RESOURCE")
ipt_username <- Sys.getenv("IPT_USER");     if (!nzchar(ipt_username)) stop("Missing env var: IPT_USER")
ipt_password <- Sys.getenv("IPT_PASS");     if (!nzchar(ipt_password)) stop("Missing env var: IPT_PASS")

ipt_published <- (function() {

  publish_timeout_min <- 20   # minutes to wait for the publication to finish
  poll_interval_sec   <- 5

  `%||%` <- function(x, y) {
    if (!is.null(x) && length(x) > 0 && !is.na(x[1]) && nzchar(x[1])) x else y
  }

  ask_yes_no <- function(prompt, default = FALSE) {
    if (!interactive()) {
      message(prompt)
      message("Non-interactive session - defaulting to: ", if (default) "yes" else "no")
      return(default)
    }
    tolower(trimws(readline(paste0(prompt, " [y/N]: ")))) %in% c("y", "yes", "j", "ja")
  }

  # --- QA gate -------------------------------------------------------------

  if (exists("dwc", inherits = TRUE) && nrow(dwc) == 0) {
    cat("\nDwC table has 0 rows - refusing to publish an empty dataset.\n")
    return(FALSE)
  }

  qa_blockers <- character(0)

  if (exists("n_dup_ids", inherits = TRUE) && !is.na(n_dup_ids) && n_dup_ids > 0) {
    affected <- if (exists("n_dup_rows", inherits = TRUE) && !is.na(n_dup_rows)) {
      as.character(n_dup_rows)
    } else {
      "?"
    }
    qa_blockers <- c(qa_blockers,
                     sprintf("Duplicate occurrenceID: %d ids affecting %s rows", n_dup_ids, affected))
  }

  if (exists("n_bad_sweref", inherits = TRUE) && !is.na(n_bad_sweref) && n_bad_sweref > 0) {
    qa_blockers <- c(qa_blockers, sprintf("Invalid SWEREF99 coordinates: %d rows", n_bad_sweref))
  }

  if (exists("n_bad_rt90", inherits = TRUE) && !is.na(n_bad_rt90) && n_bad_rt90 > 0) {
    qa_blockers <- c(qa_blockers, sprintf("Invalid RT90 coordinates: %d rows", n_bad_rt90))
  }

  if (exists("n_bad_media", inherits = TRUE) && !is.na(n_bad_media) && n_bad_media > 0) {
    qa_blockers <- c(qa_blockers, sprintf("Broken media links: %d rows", n_bad_media))
  }

  if (length(qa_blockers) > 0) {
    cat("\n--- QA warnings ------------------------------------------------------\n")
    for (issue in qa_blockers) cat("  !", issue, "\n")
    cat("\n")
    if (!ask_yes_no("Publish to IPT anyway?")) {
      cat("Not proceeding past QA warnings. Publishing cancelled.\n")
      return(FALSE)
    }
    cat("Proceeding despite QA warnings.\n\n")
  } else {
    cat("\n--- QA gate: no issues found -----------------------------------------\n")
  }

  # --- Endpoints and version comment ------------------------------------

  base_url    <- sub("/+$", "", ipt_base_url)
  login_url   <- paste0(base_url, "/login.do")
  publish_url <- paste0(base_url, "/manage/publish.do")
  report_url  <- paste0(base_url, "/manage/report.do")
  rss_url     <- paste0(base_url, "/rss.do")
  r_param     <- utils::URLencode(ipt_resource, reserved = TRUE)

  ipt_comment <- "Pipeline publication."   # a free-text note may be appended below

  read_ipt_version <- function(h) {
    tryCatch({
      resp <- httr::GET(rss_url, handle = h, httr::timeout(60))
      if (httr::status_code(resp) >= 400) return(NA_character_)
      txt   <- httr::content(resp, as = "text", encoding = "UTF-8")
      items <- regmatches(txt, gregexpr("<item>.*?</item>", txt, perl = TRUE))[[1]]
      hit   <- items[grepl(sprintf("[?&]r=%s(&|<|\"|')", ipt_resource), items)]
      if (length(hit) == 0) return(NA_character_)
      v <- regmatches(hit[1], regexpr("[?&]v=([0-9.]+)", hit[1], perl = TRUE))
      if (length(v) == 0) return(NA_character_)
      sub(".*v=", "", v)
    }, error = function(e) NA_character_)
  }

  # --- Final confirmation ----------------------------------------------

  cat("\n--- Ready to publish ----------------------------------------------------\n")
  cat("IPT:              ", base_url,     "\n", sep = "")
  cat("Resource:         ", ipt_resource, "\n", sep = "")
  cat("User:             ", ipt_username, "\n\n", sep = "")

  is_prod <- !grepl("test", tolower(base_url), fixed = TRUE)

  if (interactive()) {
    if (is_prod) {
      cat("*** PRODUCTION *** - this publishes to ", base_url, "\n", sep = "")
      # Loop so buffered console input (e.g. from running via a pasted
      # source() line) does not decide this - re-ask until PROD or a blank
      # line. A blank line aborts.
      repeat {
        ans <- trimws(readline("Type PROD to confirm, or blank to abort: "))
        if (identical(ans, "PROD")) break
        if (!nzchar(ans)) {
          cat("Aborted - production publish not confirmed.\n")
          return(FALSE)
        }
        cat("Not confirmed. Type PROD exactly, or leave blank to abort.\n")
      }
    } else if (!ask_yes_no("Publish a new version of this resource now?")) {
      cat("Aborted at final confirmation.\n")
      return(FALSE)
    }

    note <- trimws(readline("Note for this version (blank = none): "))
    if (nzchar(note)) ipt_comment <- paste0(ipt_comment, " ", note)
    cat("Version comment:  ", ipt_comment, "\n", sep = "")
  }

  # --- Session: CSRF token + login -----------------------------------

  h <- httr::handle(base_url)

  home_resp <- httr::GET(base_url, handle = h, httr::timeout(60))
  if (httr::status_code(home_resp) >= 400) {
    stop(sprintf("Could not reach IPT at %s (HTTP %d).", base_url, httr::status_code(home_resp)))
  }

  ck   <- httr::cookies(home_resp)
  csrf <- ck$value[ck$name == "CSRFtoken"]
  if (length(csrf) == 0 || !nzchar(csrf[1])) stop("No CSRFtoken cookie returned by the IPT.")

  httr::POST(
    login_url,
    body   = list(email = ipt_username, password = ipt_password, csrfToken = csrf[1], login = "Login"),
    encode = "form", handle = h, httr::timeout(60)
  )

  check_body <- httr::content(
    httr::GET(paste0(base_url, "/manage/resources.do"), handle = h, httr::timeout(60)),
    as = "text", encoding = "UTF-8"
  )
  if (grepl('name="password"', check_body, fixed = TRUE)) {
    stop("IPT login failed - check IPT_USER / IPT_PASS.")
  }
  cat("IPT login: OK\n")

  # --- Verify the account manages this resource ---------------------
  # The Manage Resources list is rendered client-side, so its raw HTML
  # never contains the shortname. Check the resource's own overview page.

  res_resp <- httr::GET(paste0(base_url, "/manage/resource.do?r=", r_param),
                        handle = h, httr::timeout(60))
  res_body <- httr::content(res_resp, as = "text", encoding = "UTF-8")

  if (httr::status_code(res_resp) >= 400 ||
      grepl("/manage/resources\\.do", res_resp$url) ||
      !grepl(ipt_resource, res_body, fixed = TRUE)) {
    stop(sprintf(paste0(
      "Account %s cannot manage resource '%s' on %s (HTTP %d). ",
      "Check IPT_RESOURCE / IPT_USER in .Renviron."),
      ipt_username, ipt_resource, base_url, httr::status_code(res_resp)))
  }

  current_version <- read_ipt_version(h)
  cat("Current IPT version: ", current_version %||% "unknown", "\n", sep = "")

  # --- Publish -----------------------------------------------------

  pub_resp <- httr::POST(
    paste0(publish_url, "?r=", r_param),
    body   = list(r = ipt_resource, summary = ipt_comment, publish = "Publish"),
    encode = "form", handle = h, httr::timeout(120)
  )
  if (httr::status_code(pub_resp) >= 400) {
    stop(sprintf("IPT publish request failed (HTTP %d): %s",
                 httr::status_code(pub_resp),
                 substr(httr::content(pub_resp, as = "text", encoding = "UTF-8"), 1, 400)))
  }

  # --- Poll the publication report -----------------------------

  cat("Publication started - polling report (timeout ", publish_timeout_min, " min)...\n", sep = "")
  deadline <- Sys.time() + publish_timeout_min * 60
  status   <- "timeout"
  rep_body <- ""

  repeat {
    Sys.sleep(poll_interval_sec)
    rep_body <- httr::content(
      httr::GET(paste0(report_url, "?r=", r_param), handle = h, httr::timeout(60)),
      as = "text", encoding = "UTF-8"
    )
    if (grepl('class="completed"', rep_body, fixed = TRUE)) {
      status <- if (grepl("alert-danger", rep_body, fixed = TRUE) ||
                    grepl("text-gbif-danger", rep_body, fixed = TRUE)) "failed" else "completed"
      break
    }
    if (Sys.time() > deadline) break
    cat("  ... still publishing\n")
  }

  # --- Report + sanity checks -------------------------------

  published_n <- {
    m <- regmatches(rep_body,
                    gregexpr("[0-9][0-9,]{0,12}(?=\\s*records?\\b)", rep_body,
                             perl = TRUE, ignore.case = TRUE))[[1]]
    if (length(m) == 0) NA_integer_ else suppressWarnings(max(as.integer(gsub(",", "", m)), na.rm = TRUE))
  }

  ref_n <- if (exists("n_rows", inherits = TRUE) && !is.na(n_rows)) {
    as.integer(n_rows)
  } else if (exists("dwc", inherits = TRUE)) {
    nrow(dwc)
  } else {
    NA_integer_
  }

  new_version <- read_ipt_version(h)

  cat("\n--- IPT publish ", toupper(status), " ",
      strrep("-", max(0, 45 - nchar(status))), "\n", sep = "")
  cat("Resource:          ", ipt_resource,                   "\n", sep = "")
  cat("Previous version:  ", current_version %||% "unknown", "\n", sep = "")
  cat("Current version:   ", new_version     %||% "unknown", "\n", sep = "")
  cat("Records published: ",
      if (is.na(published_n)) "unknown" else format(published_n, big.mark = " "), "\n", sep = "")

  if (identical(status, "failed")) {
    stop("IPT reported a failed publication - check the resource's log in the IPT UI.")
  }
  if (identical(status, "timeout")) {
    warning(sprintf("Publication did not finish within %d min - check the IPT UI.", publish_timeout_min))
    return(FALSE)
  }
  if (!is.na(published_n) && published_n == 0) {
    stop("IPT published 0 records - the Darwin Core mapping or the source is missing ",
         "or empty. Fix it in the IPT and publish again.")
  }
  if (!is.na(published_n) && !is.na(ref_n) && ref_n > 0 && published_n < ref_n / 2) {
    stop(sprintf(paste0(
      "IPT published %s records but this session's DwC table has %s - too far below. ",
      "Check the IPT source and mapping before trusting this version."),
      format(published_n, big.mark = " "), format(ref_n, big.mark = " ")))
  }

  cat("\nGBIF will re-harvest ", ipt_resource, " on its next crawl.\n", sep = "")
  TRUE
})()
