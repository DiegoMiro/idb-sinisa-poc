# ============================================================
# 03_compare_sinisa_ibge.R
# ------------------------------------------------------------
# Purpose:
#   - Compare SINISA municipal data with the IBGE municipal
#     reference table to verify consistency of:
#       * Municipal code (cod_IBGE)
#       * Municipality name
#       * UF
#       * Macrorregião
#
# Context:
#   - SINISA municipal base (Gestão Técnica Água – Base Municipal)
#     contains its own identification of municipalities.
#
#   - This project uses a IBGE reference table:
#       data/processed/ibge_municipalities.csv
#
#   This CSV is produced previously in the workflow and already
#   contains a clean, unique municipal lookup table with:
#       * cod_IBGE     (character)
#       * Município    (municipality name)
#       * UF           (state name, not UF sigla)
#       * Macrorregião (IBGE macroregion)
#
# Inputs expected (tracked in Git):
#   - data/processed/gestao_tecnica_agua.csv
#   - data/processed/ibge_municipalities.csv
#
# Outputs produced:
#   - data/processed/compare_sinisa_ibge_full.csv
#       Full joined table SINISA x IBGE
#
#   - data/processed/compare_sinisa_ibge_uf_cross_tab.csv
#       Cross tab comparing UF (SINISA vs IBGE)
#
#   - data/processed/compare_sinisa_ibge_macro_cross_tab.csv
#       Cross tab comparing Macrorregião (SINISA vs IBGE)
#
#   - data/processed/diff_municipalities.csv
#       Municipalities where municipality name differs
#       between SINISA and IBGE, but cod_IBGE is the same.
#
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(fs)
  library(logger)
  library(stringr)
  library(tidyr)
})

# -------------------------------
# Paths
# -------------------------------
PROCESSED_PATH      <- "data/processed"
SINISA_MUN_PATH     <- fs::path(PROCESSED_PATH, "gestao_tecnica_agua.csv")
IBGE_MUN_CSV_PATH   <- fs::path(PROCESSED_PATH, "ibge_municipalities.csv")

if (!fs::dir_exists(PROCESSED_PATH)) fs::dir_create(PROCESSED_PATH)

# -------------------------------
# Logging setup
# -------------------------------
log_dir  <- "logs"
log_file <- fs::path(log_dir, "ibge_compare.log")

if (!fs::dir_exists(log_dir)) fs::dir_create(log_dir)

log_layout(
  layout_glue_generator(
    format = "[{format(time, '%Y-%m-%d %H:%M:%S')}] {toupper(level)}: {msg}"
  )
)

log_appender(appender_tee(log_file))


# ============================================================
# Main function
# ============================================================
compare_sinisa_ibge <- function(context = "SINISA_IBGE_COMPARE") {
  
  ctx <- context
  log_info("[{ctx}] ==== Starting SINISA x IBGE municipalities comparison ====")
  
  # ----------------------------------------------------------
  # 1) Check inputs
  # ----------------------------------------------------------
  if (!fs::file_exists(SINISA_MUN_PATH)) {
    log_error("[{ctx}] SINISA processed file not found: {SINISA_MUN_PATH}")
    stop(
      "SINISA processed file not found. Expected at: ",
      SINISA_MUN_PATH,
      ". Run 02_prepare_and_compute_sinisa.R first."
    )
  }
  
  if (!fs::file_exists(IBGE_MUN_CSV_PATH)) {
    log_error("[{ctx}] IBGE municipal reference CSV not found: {IBGE_MUN_CSV_PATH}")
    stop(
      "IBGE municipal reference CSV not found. Expected at: ",
      IBGE_MUN_CSV_PATH,
      ". Ensure 03_build_ibge_municipal_table.R has been executed."
    )
  }
  
  log_info("[{ctx}] Using SINISA base (municipal): {SINISA_MUN_PATH}")
  log_info("[{ctx}] Using IBGE reference table: {IBGE_MUN_CSV_PATH}")
  
  # ----------------------------------------------------------
  # 2) Read IBGE municipal reference
  # ----------------------------------------------------------
  ibge_municipalities <- read_csv(IBGE_MUN_CSV_PATH, show_col_types = FALSE) %>%
    mutate(cod_IBGE = as.character(cod_IBGE))
  
  log_info(
    "[{ctx}] IBGE municipal reference loaded: {nrow(ibge_municipalities)} row(s)."
  )
  
  # ----------------------------------------------------------
  # 3) Read SINISA municipal base
  # ----------------------------------------------------------
  sinisa_municipal <- read_csv(SINISA_MUN_PATH, show_col_types = FALSE) %>%
    mutate(cod_IBGE = as.character(cod_IBGE))
  
  log_info("[{ctx}] SINISA municipal base loaded: {nrow(sinisa_municipal)} row(s).")
  
  # ----------------------------------------------------------
  # 4) Join SINISA x IBGE by cod_IBGE
  # ----------------------------------------------------------
  log_info("[{ctx}] Joining SINISA and IBGE by cod_IBGE (character).")
  
  df_compare_ibge_columns <- sinisa_municipal %>%
    select(cod_IBGE, Macrorregião, Município, UF) %>%
    left_join(
      ibge_municipalities,
      by = "cod_IBGE",
      suffix = c("__SINISA", "__IBGE")
    ) %>%
    select(
      cod_IBGE,
      Município__SINISA,
      Município__IBGE,
      Macrorregião__SINISA,
      Macrorregião__IBGE,
      UF__SINISA,
      UF__IBGE
    )
  
  compare_full_path <- fs::path(PROCESSED_PATH, "compare_sinisa_ibge_full.csv")
  write_csv(df_compare_ibge_columns, compare_full_path)
  log_info("[{ctx}] Full SINISA x IBGE comparison written to: {compare_full_path}")
  
  # ----------------------------------------------------------
  # 5) UF and Macrorregião sanity checks
  # ----------------------------------------------------------
  uf_compare <- df_compare_ibge_columns %>%
    count(UF__SINISA, UF__IBGE, sort = TRUE)
  
  uf_compare_path <- fs::path(PROCESSED_PATH, "compare_sinisa_ibge_uf_cross_tab.csv")
  write_csv(uf_compare, uf_compare_path)
  log_info("[{ctx}] UF cross-tab written to: {uf_compare_path}")
  
  macro_compare <- df_compare_ibge_columns %>%
    count(Macrorregião__SINISA, Macrorregião__IBGE, sort = TRUE)
  
  macro_compare_path <- fs::path(PROCESSED_PATH, "compare_sinisa_ibge_macro_cross_tab.csv")
  write_csv(macro_compare, macro_compare_path)
  log_info("[{ctx}] Macrorregião cross-tab written to: {macro_compare_path}")
  
  # ----------------------------------------------------------
  # 6) Municipality name differences
  # ----------------------------------------------------------
  log_info("[{ctx}] Identifying municipality name mismatches.")
  
  diff_municipalities <- df_compare_ibge_columns %>%
    select(cod_IBGE, starts_with("Município")) %>%
    filter(
      !is.na(Município__SINISA),
      !is.na(Município__IBGE),
      Município__SINISA != Município__IBGE
    )
  
  diff_mun_path <- fs::path(PROCESSED_PATH, "diff_municipalities.csv")
  write_csv(diff_municipalities, diff_mun_path)
  log_info("[{ctx}] diff_municipalities written to: {diff_mun_path}")
  
  log_info("[{ctx}] ==== SINISA x IBGE comparison finished successfully ====")
  
  invisible(
    list(
      ibge_municipalities = ibge_municipalities,
      df_compare_ibge     = df_compare_ibge_columns,
      uf_compare          = uf_compare,
      macro_compare       = macro_compare,
      diff_municipalities = diff_municipalities
    )
  )
}


# ============================================================
# Run
# ============================================================
log_info("[SINISA_IBGE_COMPARE] Script executed directly. Running compare_sinisa_ibge().")
compare_sinisa_ibge()
