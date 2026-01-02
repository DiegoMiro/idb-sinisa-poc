# ============================================================
# 03_build_ibge_municipal_table.R
# ------------------------------------------------------------
# Purpose:
#   - Download IBGE DTB 2023 (if needed)
#   - Unzip DTB_2023.zip into data/raw/
#   - Read RELATORIO_DTB_BRASIL_MUNICIPIO.xls
#   - Build a municipal reference table:
#       * cod_IBGE     (Código Município Completo)
#       * Município    (Nome_Município)
#       * UF           (Nome_UF ou UF)
#       * Macrorregião (Nome_Região Geográfica)
#       * uf_id        (código da UF, se existir)
#   - Save as a compact CSV to be tracked in Git
#   - Log the whole process to logs/ibge_build_ref.log
#
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(readxl)
  library(fs)
  library(logger)
})

# -------------------------------
# Paths & constants
# -------------------------------
DATA_RAW_PATH   <- "data/raw"
PROCESSED_PATH  <- "data/processed"

DTB_ZIP_URL   <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/divisao_territorial/2023/DTB_2023.zip"
DTB_ZIP_PATH  <- fs::path(DATA_RAW_PATH, "DTB_2023.zip")
DTB_DIR_PATH  <- fs::path(DATA_RAW_PATH, "DTB_2023")
DTB_XLS_PATH  <- fs::path(DTB_DIR_PATH, "RELATORIO_DTB_BRASIL_MUNICIPIO.xls")

if (!fs::dir_exists(DATA_RAW_PATH))  fs::dir_create(DATA_RAW_PATH)
if (!fs::dir_exists(PROCESSED_PATH)) fs::dir_create(PROCESSED_PATH)

# -------------------------------
# Logging setup
# -------------------------------
log_dir  <- "logs"
log_file <- fs::path(log_dir, "ibge_build_ref.log")

if (!fs::dir_exists(log_dir)) fs::dir_create(log_dir)

log_layout(
  layout_glue_generator(
    format = "[{format(time, '%Y-%m-%d %H:%M:%S')}] {toupper(level)}: {msg}"
  )
)

log_appender(appender_tee(log_file))


# ============================================================
# Helper: garantir DTB baixado e descompactado
# ============================================================
ensure_dtb_2023_available <- function(ctx = "IBGE_REF_BUILD") {
  
  # 1) Baixar ZIP se ainda não existir
  if (!fs::file_exists(DTB_ZIP_PATH)) {
    log_info("[{ctx}] DTB_2023.zip not found locally. Downloading from IBGE...")
    log_info("[{ctx}] URL: {DTB_ZIP_URL}")
    
    tryCatch(
      {
        download.file(
          url      = DTB_ZIP_URL,
          destfile = DTB_ZIP_PATH,
          mode     = "wb",
          quiet    = TRUE
        )
        log_info("[{ctx}] Download completed: {DTB_ZIP_PATH}")
      },
      error = function(e) {
        log_error("[{ctx}] Failed to download DTB_2023.zip: {conditionMessage(e)}")
        stop(
          "Failed to download DTB_2023.zip from IBGE. ",
          "Check your internet connection or download it manually to: ",
          DTB_ZIP_PATH
        )
      }
    )
  } else {
    log_info("[{ctx}] Found existing ZIP at: {DTB_ZIP_PATH}")
  }
  
  # 2) Descompactar se o XLS ainda não existir
  if (!fs::file_exists(DTB_XLS_PATH)) {
    log_info("[{ctx}] Excel file not found. Unzipping DTB_2023.zip into: {DATA_RAW_PATH}")
    
    tryCatch(
      {
        unzip(DTB_ZIP_PATH, exdir = DATA_RAW_PATH)
        log_info("[{ctx}] Unzip completed.")
      },
      error = function(e) {
        log_error("[{ctx}] Failed to unzip DTB_2023.zip: {conditionMessage(e)}")
        stop(
          "Failed to unzip DTB_2023.zip. ",
          "Please unzip it manually into data/raw/ so that the directory ",
          "DTB_2023/ and the file RELATORIO_DTB_BRASIL_MUNICIPIO.xls exist."
        )
      }
    )
  } else {
    log_info("[{ctx}] Excel file already present at: {DTB_XLS_PATH}")
  }
  
  # 3) Checar se o XLS realmente existe após o unzip
  if (!fs::file_exists(DTB_XLS_PATH)) {
    log_error("[{ctx}] After unzip, Excel file still not found at: {DTB_XLS_PATH}")
    stop(
      "RELATORIO_DTB_BRASIL_MUNICIPIO.xls not found after unzip. ",
      "Check the internal structure of DTB_2023.zip and adjust DTB_XLS_PATH if needed."
    )
  }
}


# ============================================================
# Main function
# ============================================================
#' Build IBGE municipal reference table from DTB 2023 (RELATORIO_DTB_BRASIL_MUNICIPIO.xls)
#'
#' @param context Logging context label
#'
#' @return invisibly returns the tibble with municipalities
#'
build_ibge_municipal_table <- function(context = "IBGE_REF_BUILD") {
  
  ctx <- context
  log_info("[{ctx}] ==== Starting IBGE municipal reference build (DTB 2023) ====")
  
  # Garante download + unzip
  ensure_dtb_2023_available(ctx = ctx)
  
  log_info("[{ctx}] Reading DTB Excel file: {DTB_XLS_PATH}")
  dtb_raw <- readxl::read_excel(DTB_XLS_PATH) %>%
    rowid_to_column() %>%
    filter(.[[2]] == "UF") %>%
    pull(rowid) %>%
    {
      n = . - 1
      readxl::read_excel(DTB_XLS_PATH, skip = n)  
    }
  log_info("[{ctx}] DTB Excel loaded with {nrow(dtb_raw)} row(s) and {ncol(dtb_raw)} column(s).")
  
  # Ajuste os nomes de coluna aqui se forem diferentes na sua planilha
  # Exemplos típicos:
  # - "Código Município Completo"
  # - "Nome_Município"
  # - "UF" ou "Nome_UF"
  # - "Nome_Região Geográfica"
  # - "Código_UF"
  
  if (!"Código Município Completo" %in% names(dtb_raw)) {
    stop(
      "A coluna 'Código Município Completo' não foi encontrada na planilha. ",
      "Execute names(readxl::read_excel(DTB_XLS_PATH)) para inspecionar ",
      "os nomes reais e ajuste o código."
    )
  }
  if (!"Nome_Município" %in% names(dtb_raw)) {
    stop(
      "A coluna 'Nome_Município' não foi encontrada na planilha. ",
      "Execute names(readxl::read_excel(DTB_XLS_PATH)) para inspecionar ",
      "os nomes reais e ajuste o código."
    )
  }
  
  ibge_municipalities <- dtb_raw %>%
    select(
      cod_IBGE  = `Código Município Completo`,
      Município = Nome_Município,
      UF        = Nome_UF
    ) %>%
    filter(!is.na(cod_IBGE)) %>%
    distinct() %>%
    left_join(
      tribble(
        ~UF,                  ~Macrorregião,
        "Rondônia",           "Norte",
        "Acre",               "Norte",
        "Amazonas",           "Norte",
        "Roraima",            "Norte",
        "Pará",               "Norte",
        "Amapá",              "Norte",
        "Tocantins",          "Norte",
        "Maranhão",           "Nordeste",
        "Piauí",              "Nordeste",
        "Ceará",              "Nordeste",
        "Rio Grande do Norte","Nordeste",
        "Paraíba",            "Nordeste",
        "Pernambuco",         "Nordeste",
        "Alagoas",            "Nordeste",
        "Sergipe",            "Nordeste",
        "Bahia",              "Nordeste",
        "Minas Gerais",       "Sudeste",
        "Espírito Santo",     "Sudeste",
        "Rio de Janeiro",     "Sudeste",
        "São Paulo",          "Sudeste",
        "Paraná",             "Sul",
        "Santa Catarina",     "Sul",
        "Rio Grande do Sul",  "Sul",
        "Mato Grosso do Sul", "Centro-Oeste",
        "Mato Grosso",        "Centro-Oeste",
        "Goiás",              "Centro-Oeste",
        "Distrito Federal",   "Centro-Oeste"
      ),
      "UF"
    )
  
  log_info(
    "[{ctx}] Municipal reference built with {nrow(ibge_municipalities)} unique municipality row(s)."
  )
  
  # 5) Save compact CSV (this is what goes to Git)
  out_path <- fs::path(PROCESSED_PATH, "ibge_municipalities.csv")
  write_csv(ibge_municipalities, out_path)
  
  log_info("[{ctx}] IBGE municipal reference written to: {out_path}")
  log_info("[{ctx}] ==== IBGE municipal reference build finished successfully ====")
  
  invisible(ibge_municipalities)
}


# ============================================================
# Run
# ============================================================
log_info("[IBGE_REF_BUILD] Script executed directly. Running build_ibge_municipal_table().")
build_ibge_municipal_table()
