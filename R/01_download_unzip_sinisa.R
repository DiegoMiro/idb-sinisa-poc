# ============================================================
# 01_download_unzip_sinisa.R
# ------------------------------------------------------------
# Purpose:
#   - Download the official SINISA Water ZIP file (2023)
#   - Save it under data/raw/
#   - Validate and unzip into data/raw/extracted/
#   - Log the whole process to logs/download_sinisa.log
#
# ============================================================

suppressPackageStartupMessages({
  library(fs)
  library(stringr)
  library(glue)
  library(logger)
  library(purrr)
  library(zip)
  library(archive)
})

# -------------------------------
# Logging setup
# -------------------------------

# Path to save logs
log_dir  <- "logs"
log_file <- fs::path(log_dir, "download_sinisa.log")

# Ensure logs directory exists
if (!fs::dir_exists(log_dir)) fs::dir_create(log_dir)

# Layout with timestamp, level and message
log_layout(
  layout_glue_generator(
    format = "[{format(time, '%Y-%m-%d %H:%M:%S')}] {toupper(level)}: {msg}"
  )
)

# Log to file and console at the same time
log_appender(appender_tee(log_file))


# -------------------------------
# Project paths
# -------------------------------
RAW_PATH       <- "data/raw"
EXTRACTED_PATH <- "data/raw/extracted"

# Ensure directories exist
if (!fs::dir_exists(RAW_PATH))       fs::dir_create(RAW_PATH)
if (!fs::dir_exists(EXTRACTED_PATH)) fs::dir_create(EXTRACTED_PATH)

zip_file_name <- "SINISA_AGUA_2023_v2.1.1.zip"
zip_file_path <- fs::path(RAW_PATH, zip_file_name)


# ============================================================
# Function: Download SINISA ZIP
# ============================================================
#' Download SINISA Water ZIP file (2023)
#'
#' @param context Optional string used for logging context
#'
#' @return Invisibly returns the local file path of the ZIP
#'
download_sinisa_zip <- function(context = "SINISA_ZIP_DOWNLOAD") {
  
  ctx <- context
  
  # Build URL by joining segments with "/"
  url <- c(
    "https://www.gov.br",
    "cidades",
    "pt-br",
    "acesso-a-informacao",
    "acoes-e-programas",
    "saneamento",
    "sinisa",
    "arquivos",
    "SINISA_AGUA_Planilhas_2023_v2.1.1.zip"
  ) %>%
    stringr::str_c(collapse = "/")
  
  file_path <- zip_file_path
  
  log_info("[{ctx}] Download process initialized.")
  log_info("[{ctx}] Target file: {file_path}")
  
  # ----------------------------------------------------------
  # If ZIP already exists, skip download
  # ----------------------------------------------------------
  if (fs::file_exists(file_path)) {
    size_mb <- fs::file_info(file_path)$size / (1024^2)
    log_info("[{ctx}] File already exists ({round(size_mb, 2)} MB). Skipping download.")
    log_info("[{ctx}] Process finished (CACHE HIT).")
    return(invisible(file_path))
  }
  
  # ----------------------------------------------------------
  # Download
  # ----------------------------------------------------------
  log_info("[{ctx}] Starting SINISA ZIP download.")
  log_info("[{ctx}] Source URL: {url}")
  
  ok <- tryCatch({
    utils::download.file(
      url,
      destfile = file_path,
      mode     = "wb",
      quiet    = FALSE,
      method   = "libcurl"
    )
    TRUE
  }, error = function(e) {
    log_error("[{ctx}] Download failed. Check connection or URL.")
    log_error("[{ctx}] Error message: {e$message}")
    FALSE
  })
  
  if (!ok) {
    log_error("[{ctx}] Process aborted due to error.")
    stop("SINISA ZIP download unsuccessful.")
  }
  
  size_mb <- fs::file_info(file_path)$size / (1024^2)
  log_info("[{ctx}] Download completed successfully ({round(size_mb, 2)} MB).")
  log_info("[{ctx}] File saved to: {file_path}")
  log_info("[{ctx}] Process finished (SUCCESS).")
  
  invisible(file_path)
}


# ============================================================
# Function: Unzip SINISA ZIP using archive::archive_extract()
# ============================================================
#' Unzip SINISA 2023 Water Dataset
#'
#' @param context Optional logging label
#'
#' @return Invisibly returns the directory containing extracted files
#'
unzip_sinisa <- function(context = "SINISA_UNZIP") {
  
  ctx <- context
  
  log_info("[{ctx}] Unzip process initialized.")
  log_info("[{ctx}] Expected ZIP file: {zip_file_path}")
  
  # Ensure ZIP exists (download if missing)
  if (!fs::file_exists(zip_file_path)) {
    log_warn("[{ctx}] ZIP file not found. Calling download_sinisa_zip() first.")
    download_sinisa_zip(context = ctx)
  } else {
    size_mb <- fs::file_info(zip_file_path)$size / (1024^2)
    log_info("[{ctx}] ZIP file found ({round(size_mb, 2)} MB).")
  }
  
  # Validate ZIP with zip::zip_list()
  log_info("[{ctx}] Validating ZIP using zip::zip_list().")
  
  valid_zip <- tryCatch({
    info <- zip::zip_list(zip_file_path)
    log_info("[{ctx}] ZIP validation succeeded. {nrow(info)} item(s) found inside ZIP.")
    purrr::walk(
      head(info$filename, 10),
      \(f) log_debug("[{ctx}] ZIP contains: {f}")
    )
    TRUE
  }, error = function(e) {
    log_error("[{ctx}] Error while listing ZIP contents: {e$message}")
    FALSE
  })
  
  if (!valid_zip) {
    log_warn("[{ctx}] ZIP invalid. Re-downloading and retrying.")
    download_sinisa_zip(context = ctx)
    
    valid_zip <- tryCatch({
      info <- zip::zip_list(zip_file_path)
      log_info("[{ctx}] ZIP validation succeeded after re-download. {nrow(info)} item(s).")
      TRUE
    }, error = function(e) {
      log_error("[{ctx}] Error while listing ZIP contents after re-download: {e$message}")
      FALSE
    })
    
    if (!valid_zip) {
      log_error("[{ctx}] ZIP still invalid after re-download. Aborting.")
      stop("SINISA ZIP appears corrupted.")
    }
  }
  
  # Cache check: if extracted folder already has content
  existing_files <- fs::dir_ls(EXTRACTED_PATH, recurse = FALSE, fail = FALSE)
  
  if (length(existing_files) > 0) {
    log_info("[{ctx}] Extracted files already exist. Skipping unzip (CACHE HIT).")
    log_info("[{ctx}] {length(existing_files)} existing file(s) in: {EXTRACTED_PATH}")
    log_info("[{ctx}] Process finished.")
    return(invisible(EXTRACTED_PATH))
  }
  
  # Extract using archive (other functions failed here like utils::unzip)
  log_info("[{ctx}] Extracting with archive::archive_extract().")
  log_info("[{ctx}] Destination: {EXTRACTED_PATH}")
  
  ok_unzip <- tryCatch({
    archive::archive_extract(zip_file_path, dir = EXTRACTED_PATH)
    TRUE
  }, error = function(e) {
    log_error("[{ctx}] Extraction failed: {e$message}")
    FALSE
  })
  
  if (!ok_unzip) {
    log_error("[{ctx}] Unzip failed. Aborting.")
    stop("Unzip unsuccessful.")
  }
  
  # Summary
  extracted_files <- fs::dir_ls(EXTRACTED_PATH, recurse = TRUE)
  
  log_info("[{ctx}] Unzip completed successfully.")
  log_info("[{ctx}] {length(extracted_files)} file(s) extracted.")
  
  purrr::walk(
    extracted_files,
    \(f) log_debug("[{ctx}] Extracted: {f}")
  )
  
  log_info("[{ctx}] Process finished (SUCCESS).")
  
  invisible(EXTRACTED_PATH)
}


# ============================================================
# Run
# ============================================================
log_info("[SINISA_PIPELINE] Script executed directly. Running download_sinisa_zip() and unzip_sinisa().")
download_sinisa_zip()
unzip_sinisa()
