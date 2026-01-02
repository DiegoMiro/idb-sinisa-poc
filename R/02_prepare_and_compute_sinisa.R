# ============================================================
# 02_prepare_and_compute_sinisa.R
# ------------------------------------------------------------
# Purpose:
#   - Read all necessary SINISA Excel files from data/raw/extracted/
#   - Build metadata tables (column descriptions, index formulas, dependencies)
#   - Compute indices safely with exception rules
#   - Compare calculated vs official indices
#   - Export all outputs as CSVs under data/processed/
#   - Log the whole process to logs/indicator_calc.log
#
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(readxl)
  library(fs)
  library(janitor)
  library(rlang)
  library(logger)
  library(magrittr)
  library(lubridate)
})

# -------------------------------
# Paths
# -------------------------------
RAW_EXTRACTED_PATH <- "data/raw/extracted"
PROCESSED_PATH     <- "data/processed"

if (!fs::dir_exists(RAW_EXTRACTED_PATH)) fs::dir_create(RAW_EXTRACTED_PATH)
if (!fs::dir_exists(PROCESSED_PATH))     fs::dir_create(PROCESSED_PATH)

# -------------------------------
# Logging setup
# -------------------------------
log_dir  <- "logs"
log_file <- fs::path(log_dir, "indicator_calc.log")  # log único para prep + cálculo

if (!fs::dir_exists(log_dir)) fs::dir_create(log_dir)

log_layout(
  layout_glue_generator(
    format = "[{format(time, '%Y-%m-%d %H:%M:%S')}] {toupper(level)}: {msg}"
  )
)

log_appender(appender_tee(log_file))


# ============================================================
# Helper 1: List files ans read all sheets (df_files)
# ============================================================
build_df_files <- function(context = "SINISA_PIPELINE") {
  ctx <- context
  log_info("[{ctx}] Building df_files from: {RAW_EXTRACTED_PATH}")
  
  xlsx_paths <- dir_ls(
    RAW_EXTRACTED_PATH,
    recurse = TRUE,
    type    = "file",
    glob    = "*.xlsx$"
  )
  
  log_info("[{ctx}] Found {length(xlsx_paths)} Excel file(s).")
  
  df_files <- tibble(file_path = xlsx_paths) %>%
    mutate(
      file_path_clean = file_path %>%
        path_file() %>%
        str_extract("(?<=SINISA_AGUA_).*(?=_2023_V2)"),
      file_type = word(file_path_clean, 1, sep = "_"),
      file_name = word(file_path_clean, 2, -1, sep = "_")
    ) %>%
    # remove "_Brasil"
    filter(str_detect(file_name, "_Brasil$", negate = TRUE)) %>%
    mutate(
      sheet_name = map(file_path, excel_sheets)
    ) %>%
    unnest(sheet_name)
  
  log_info("[{ctx}] Expanded to {nrow(df_files)} (file, sheet) combination(s).")
  
  # First valid row and if have colnames
  df_files <- df_files %>%
    mutate(
      first_row = case_when(
        file_type == "Indicadores" &
          file_name == "Base Municipal" &
          sheet_name != "Nota metodológica" ~ 10,
        file_type == "Indicadores" &
          file_name == "Base Municipal" &
          sheet_name == "Nota metodológica" ~ 8,
        
        file_type == "Informacoes" &
          file_name == "Administrativo e Financeiro_Base Municipal" &
          sheet_name != "Nota metodológica" ~ 11,
        file_type == "Informacoes" &
          file_name == "Administrativo e Financeiro_Base Municipal" &
          sheet_name == "Nota metodológica" ~ 5,
        
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Base Municipal" &
          !(sheet_name %in% c("Nota metodológica", "Gestão Técnica de Água")) ~ 10,
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Base Municipal" &
          sheet_name == "Gestão Técnica de Água" ~ 11,
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Base Municipal" &
          sheet_name == "Nota metodológica" ~ 5,
        
        file_type == "Indicadores" &
          file_name == "Prestadores locais e regionais" &
          sheet_name != "Nota metodológica" ~ 10,
        file_type == "Indicadores" &
          file_name == "Prestadores locais e regionais" &
          sheet_name == "Nota metodológica" ~ 7,
        
        file_type == "Informacoes" &
          file_name == "Administrativo e Financeiro_Prestadores locais e regionais" &
          sheet_name != "Nota metodológica" ~ 11,
        file_type == "Informacoes" &
          file_name == "Administrativo e Financeiro_Prestadores locais e regionais" &
          sheet_name == "Nota metodológica" ~ 1,
        
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Prestadores locais e regionais" &
          sheet_name != "Nota metodológica" ~ 11,
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Prestadores locais e regionais" &
          sheet_name == "Nota metodológica" ~ 1,
        
        TRUE ~ 0
      ),
      col_names = case_when(
        file_type == "Informacoes" &
          file_name == "Administrativo e Financeiro_Prestadores locais e regionais" &
          sheet_name == "Nota metodológica" ~ FALSE,
        file_type == "Informacoes" &
          file_name == "Gestao Tecnica Agua_Prestadores locais e regionais" &
          sheet_name == "Nota metodológica" ~ FALSE,
        TRUE ~ TRUE
      )
    )
  
  log_info("[{ctx}] Reading Excel sheets into memory...")
  
  df_files <- df_files %>%
    mutate(
      data = pmap(
        list(
          path      = file_path,
          sheet     = sheet_name,
          first     = first_row,
          col_names = col_names
        ),
        ~ read_excel(
          path      = ..1,
          sheet     = ..2,
          skip      = ..3 - 1,
          col_names = ..4
        )
      )
    ) %>%
    rowid_to_column(var = "dataset_id") %>%
    select(-first_row, -col_names)
  
  log_info("[{ctx}] All sheets loaded successfully into df_files.")
  df_files
}


# ============================================================
# Helper 2: Columns descriptions
# ============================================================
build_description_cols <- function(df_files, context = "SINISA_PIPELINE") {
  ctx <- context
  
  log_info("[{ctx}] Building df_description_cols from df_files (dataset_id == 8 metadata).")
  
  df_description_cols <- df_files %>%
    filter(
      file_name == "Gestao Tecnica Agua_Base Municipal",
      sheet_name == "Gestão Técnica de Água"
    ) %>%
    pull(file_path) %>%
    read_excel(
      sheet     = "Gestão Técnica de Água",
      col_names = str_c("V", str_pad(1:87, width = 2, pad = "0")),
      skip      = 7,
      n_max     = 4
    ) %>%
    t() %>%
    as_tibble() %>%
    setNames(c("Category", "Description", "Unit", "Name")) %>%
    fill(Category) %>%
    select(Name, Category, Description, Unit) %>%
    mutate(
      Description = case_when(
        Description %in% c("Especifique", "Outro") ~ NA_character_,
        TRUE ~ Description
      )
    ) %>%
    rowid_to_column(var = "Column")
  
  out_path <- fs::path(PROCESSED_PATH, "description_cols.csv")
  write_csv(df_description_cols, out_path)
  
  log_info("[{ctx}] df_description_cols written to: {out_path}")
  df_description_cols
}


# ============================================================
# Helper 3: compute_columns_safely + normalize_exception_vector
# ============================================================
compute_columns_safely <- function(
    data,
    col_names,
    expr_strs,
    exception_strs = NA,
    context = NULL
) {
  
  stopifnot(length(col_names) == length(expr_strs))
  
  ctx <- if (is.null(context)) "indicator_calculation" else context
  
  log_info("[{ctx}] Starting computation of {length(col_names)} column(s).")
  
  # ---- normalizacao das excessoeos ----
  if (length(exception_strs) == 1 && is.na(exception_strs)) {
    exception_list <- vector("list", length(col_names))
  } else if (is.list(exception_strs)) {
    stopifnot(length(exception_strs) == length(col_names))
    exception_list <- exception_strs
  } else {
    stopifnot(length(exception_strs) == length(col_names))
    exception_list <- as.list(exception_strs)
  }
  
  normalize_exc_vec <- function(exc_entry) {
    if (is.null(exc_entry)) return(character(0))
    exc <- unlist(exc_entry, use.names = FALSE)
    exc <- exc[!is.na(exc) & nzchar(exc)]
    exc
  }
  
  safe_eval <- safely(
    \(expr, data) eval_tidy(expr, data = data)
  )
  
  results <- purrr::pmap(
    list(expr_strs, col_names, exception_list),
    \(expr_str, nm, exc_entry) {
      
      exc_vec <- normalize_exc_vec(exc_entry)
      
      if (length(exc_vec) > 0) {
        exc_clauses <- paste0(exc_vec, " ~ NA_real_")
        full_expr_str <- paste0(
          "case_when(",
          paste(exc_clauses, collapse = ", "),
          ", TRUE ~ (", expr_str, "))"
        )
        log_info(
          "[{ctx}] Computing `{nm}` with {length(exc_vec)} exception rule(s). Expr: {expr_str} | Exceptions: {paste(exc_vec, collapse = ' | ')}"
        )
      } else {
        full_expr_str <- expr_str
        log_info("[{ctx}] Computing `{nm}` with expression: {expr_str}")
      }
      
      expr <- parse_expr(full_expr_str)
      res  <- safe_eval(expr, data)
      
      if (!is.null(res$error)) {
        log_warn("[{ctx}] Error computing `{nm}`: {res$error$message}")
        n_exc <- NA_integer_
      } else {
        log_debug("[{ctx}] Successfully computed `{nm}`.")
        
        # log das linhas afetadas pelas exceções
        if (length(exc_vec) > 0) {
          cond_results <- purrr::map(exc_vec, \(cond_str) {
            cond_expr <- parse_expr(cond_str)
            cond_eval <- safe_eval(cond_expr, data)
            
            if (!is.null(cond_eval$error)) {
              log_warn(
                "[{ctx}] Error evaluating exception condition for `{nm}` ({cond_str}): {cond_eval$error$message}"
              )
              rep(FALSE, nrow(data))
            } else {
              as.logical(cond_eval$result)
            }
          })
          
          combined_exc <- Reduce(`|`, cond_results)
          combined_exc[is.na(combined_exc)] <- FALSE
          n_exc <- sum(combined_exc)
          
          if (n_exc > 0) {
            log_info(
              "[{ctx}] Column `{nm}`: {n_exc} row(s) matched exception rule(s) and were set to NA_real_."
            )
          } else {
            log_debug("[{ctx}] Column `{nm}`: no row matched the exception rule(s).")
          }
        } else {
          n_exc <- 0L
        }
      }
      
      list(
        name       = nm,
        result     = res,
        n_exc_rows = n_exc
      )
    }
  )
  
  new_cols <- purrr::map(results, \(x) {
    if (is.null(x$result$error)) x$result$result else NA_real_
  })
  
  new_cols_named <- set_names(new_cols, col_names)
  
  n_ok  <- sum(purrr::map_lgl(results, \(x) is.null(x$result$error)))
  n_err <- length(results) - n_ok
  
  log_info("[{ctx}] Finished computation: {n_ok} success(es), {n_err} error(s).")
  
  data %>%
    mutate(!!!new_cols_named)
}

normalize_exception_vector <- function(raw_vec) {
  map(raw_vec, \(txt) {
    
    if (is.na(txt) || str_squish(txt) == "-" || !nzchar(str_squish(txt))) {
      return(NA_character_)
    }
    
    rules <- txt %>%
      str_split("\\r\\n|\\n|\\r") %>%
      pluck(1) %>%
      str_squish() %>%
      discard(~ .x == "") %>%
      str_remove("^Não calcular indicador se\\s*") %>%
      discard(~ .x == "")
    
    if (length(rules) == 0) {
      NA_character_
    } else {
      rules
    }
  })
}


# ============================================================
# Helper 4: Index metadata
# ============================================================
build_index_metadata <- function(df_files, df_description_cols, context = "SINISA_PIPELINE") {
  ctx <- context
  
  log_info("[{ctx}] Building index metadata from Indicadores / Base Municipal / Nota metodológica.")
  
  remove_tokens <- c("\\(", "\\)", "\\+", "\\-", "\\*", "\\/") %>%
    str_c(collapse = "|")
  
  df_index_municipal <- df_files %>%
    filter(
      file_type  == "Indicadores",
      file_name  == "Base Municipal",
      sheet_name == "Nota metodológica"
    ) %>%
    pull(data) %>%
    magrittr::extract2(1) %>%
    mutate(
      index_name = CÓDIGO,
      index_formula = `FÓRMULA DE CÁLCULO` %>%
        str_replace_all("\\[|\\{", "\\(") %>%
        str_replace_all("\\]|\\}", "\\)") %>%
        str_replace_all("x|X", "*") %>%
        str_remove_all("\\."),
      index_dependence = index_formula %>%
        str_replace_all(remove_tokens, " ") %>%
        str_replace_all("\\s+", " ") %>%
        str_split(" ")
    ) %>%
    mutate(
      index_dependence = index_dependence %>%
        map(\(x) str_subset(unique(x), "^[A-Z]{3}"))
    ) %>%
    select(index_name, index_formula, index_dependence, everything())
  
  log_info("[{ctx}] index_municipal: {nrow(df_index_municipal)} index(es) found.")
  
  df_colnames <- df_files %>%
    mutate(colnames = map(data, names)) %>%
    select(dataset_id, colnames) %>%
    unnest(colnames) %>%
    summarise(
      dataset_id = list(dataset_id),
      .by        = colnames
    )
  
  df_index_possible_calculate <- df_index_municipal %>%
    select(index_name, index_dependence) %>%
    unnest(index_dependence) %>%
    left_join(
      df_colnames,
      c("index_dependence" = "colnames")
    ) %>%
    mutate(
      is_null = map_lgl(dataset_id, is.null)
    ) %>%
    summarise(
      is_possible = !any(is_null),
      .by         = index_name
    )
  
  df_index_municipal <- df_index_municipal %>%
    left_join(
      df_index_possible_calculate,
      "index_name"
    )
  
  log_info("[{ctx}] Index feasibility computed (is_possible field added).")
  
  df_index_municipal <- df_index_municipal %>%
    select(index_name, index_dependence) %>%
    unnest(index_dependence) %>%
    left_join(
      df_description_cols %>% select(index_dependence = Name, Description),
      by = "index_dependence"
    ) %>%
    mutate(
      index_dependence_description = str_c(index_dependence, ": ", Description)
    ) %>%
    summarise(
      index_dependence_description = str_c(index_dependence_description, collapse = "<br>"),
      .by = index_name
    ) %>%
    left_join(
      x  = df_index_municipal,
      by = "index_name"
    )
  
  log_info("[{ctx}] index_dependence_description built for all indices.")
  
  df_index_test <- df_index_municipal %>%
    select(index_name, index_dependence) %>%
    unnest(index_dependence) %>%
    left_join(
      df_colnames,
      by = c("index_dependence" = "colnames")
    ) %>%
    mutate(
      detect_8 = map_int(dataset_id, \(x) sum(x == 8))
    ) %>%
    summarise(
      detect_8 = min(detect_8),
      .by      = index_name
    ) %>%
    filter(detect_8 == 1) %>%
    select(-detect_8) %>%
    left_join(df_index_municipal, by = "index_name")
  
  log_info("[{ctx}] df_index_test built with {nrow(df_index_test)} index(es).")
  
  index_calc_path <- fs::path(PROCESSED_PATH, "index_calculation.csv")
  index_full_path <- fs::path(PROCESSED_PATH, "index_formula_full.csv")
  
  df_index_test %>%
    select(
      index_name,
      index_description = `NOME DO INDICADOR`,
      index_formula,
      index_dependence_description,
      index_rules = `Regras específicas para cálculo do indicador`
    ) %>%
    write_csv(index_calc_path)
  
  df_index_test %>%
    write_csv(index_full_path)
  
  log_info("[{ctx}] index_calculation.csv written to: {index_calc_path}")
  log_info("[{ctx}] index_formula_full.csv written to: {index_full_path}")
  
  list(
    df_index_municipal = df_index_municipal,
    df_index_test      = df_index_test,
    df_colnames        = df_colnames
  )
}


# ============================================================
# Helper 5: calc_fivenum to statistics (Brasil / Macrorregião / UF)
# ============================================================
calc_fivenum <- function(var_name, df_index_calculated) {
  
  v <- sym(var_name)
  
  bind_rows(
    
    summarise(
      df_index_calculated,
      n      = n(),
      na     = sum(is.na(!!v)),
      v0     = sum(!!v == 0, na.rm = TRUE),
      low    = min(!!v, na.rm = TRUE),
      q1     = quantile(!!v, 0.25, na.rm = TRUE),
      median = median(!!v, na.rm = TRUE),
      q3     = quantile(!!v, 0.75, na.rm = TRUE),
      high   = max(!!v, na.rm = TRUE),
      g100   = sum(!!v > 100, na.rm = TRUE)
    ),
    
    summarise(
      df_index_calculated,
      n      = n(),
      na     = sum(is.na(!!v)),
      v0     = sum(!!v == 0, na.rm = TRUE),
      low    = min(!!v, na.rm = TRUE),
      q1     = quantile(!!v, 0.25, na.rm = TRUE),
      median = median(!!v, na.rm = TRUE),
      q3     = quantile(!!v, 0.75, na.rm = TRUE),
      high   = max(!!v, na.rm = TRUE),
      g100   = sum(!!v > 100, na.rm = TRUE),
      .by    = `Macrorregião`
    ),
    
    summarise(
      df_index_calculated,
      n      = n(),
      na     = sum(is.na(!!v)),
      v0     = sum(!!v == 0, na.rm = TRUE),
      low    = min(!!v, na.rm = TRUE),
      q1     = quantile(!!v, 0.25, na.rm = TRUE),
      median = median(!!v, na.rm = TRUE),
      q3     = quantile(!!v, 0.75, na.rm = TRUE),
      high   = max(!!v, na.rm = TRUE),
      g100   = sum(!!v > 100, na.rm = TRUE),
      .by    = c(UF, `Macrorregião`)
    ) %>%
      arrange(`Macrorregião`)
    
  ) %>%
    mutate(
      type = case_when(
        is.na(`Macrorregião`) & is.na(UF) ~ "Brasil",
        !is.na(`Macrorregião`) & is.na(UF) ~ "Macrorregião",
        TRUE ~ "Cidade"
      ),
      `Macrorregião` = if_else(is.na(`Macrorregião`), "Brasil", `Macrorregião`),
      name = if_else(is.na(UF), `Macrorregião`, UF),
      UF   = if_else(is.na(UF), "A", UF)
    ) %>%
    arrange(`Macrorregião`, UF) %>%
    select(
      name, n, na, v0, g100,
      low, q1, median, q3, high,
      type
    )
}


# ============================================================
# Helper 6: Variable stats + repeated municipalities names
# ============================================================
build_statistics_tables <- function(df_gestao, context = "SINISA_PIPELINE") {
  
  ctx <- context
  log_info("[{ctx}] Building summary statistics tables for Gestão Técnica Água (dataset_id 8).")
  
  df_gestao <- df_gestao %>%
    mutate(cod_IBGE = as.character(cod_IBGE))
  
  stats_numeric <- df_gestao %>%
    select(where(is.numeric)) %>%
    map(
      \(x) tibble(
        n_missing = sum(is.na(x)),
        min       = min(x, na.rm = TRUE),
        max       = max(x, na.rm = TRUE),
        mean      = mean(x, na.rm = TRUE),
        cv        = sd(x, na.rm = TRUE) / mean(x, na.rm = TRUE)
      )
    ) %>%
    bind_rows(.id = "Column")
  
  numeric_path <- fs::path(PROCESSED_PATH, "statistics_numeric_cols.csv")
  write_csv(stats_numeric, numeric_path)
  log_info("[{ctx}] statistics_numeric_cols written to: {numeric_path}")
  
  stats_char <- df_gestao %>%
    select(where(is.character)) %>%
    map(
      \(x) tibble(
        n_missing        = sum(is.na(x)),
        n_valores_unicos = dplyr::n_distinct(x[!is.na(x)]),
        min_str_length   = min(str_length(x), na.rm = TRUE),
        max_str_length   = max(str_length(x), na.rm = TRUE)
      )
    ) %>%
    bind_rows(.id = "Column")
  
  char_path <- fs::path(PROCESSED_PATH, "statistics_character_cols.csv")
  write_csv(stats_char, char_path)
  log_info("[{ctx}] statistics_character_cols written to: {char_path}")
  
  repeated_mun <- df_gestao %>%
    count(Município, sort = TRUE, name = "repeated") %>%
    filter(repeated >= 2) %>%
    left_join(
      df_gestao %>%
        select(Município, cod_IBGE, UF, Macrorregião),
      by = "Município"
    )
  
  rep_path <- fs::path(PROCESSED_PATH, "repeated_municipalities_names.csv")
  write_csv(repeated_mun, rep_path)
  log_info("[{ctx}] repeated_municipalities_names written to: {rep_path}")
  
  invisible(
    list(
      stats_numeric = stats_numeric,
      stats_char    = stats_char,
      repeated_mun  = repeated_mun
    )
  )
}


# ============================================================
# Helper 7: Index calculation and validation pipeline
# ============================================================
run_indicator_pipeline <- function(
    df_files,
    df_index_test,
    context = "IDB_INDEX_V1"
) {
  
  ctx <- context
  log_info("[{ctx}] ==== Starting indicator calculation pipeline ====")
  
  base_df <- df_files$data[[8]]
  
  index_names <- df_index_test$index_name
  
  # Step 1: calcular índices com regras de exceção
  log_info("[{ctx}] Step 1: Computing indicators with compute_columns_safely().")
  
  df_index_calculated <- compute_columns_safely(
    data           = base_df,
    col_names      = df_index_test$index_name,
    expr_strs      = df_index_test$index_formula,
    exception_strs = normalize_exception_vector(
      df_index_test$`Regras específicas para cálculo do indicador`
    ),
    context        = ctx
  )
  
  # alguns casos de infinito, passando para NA
  # tem que passar pra dentro da compute_columns_safely
  df_index_calculated <- df_index_calculated %>%
    mutate(
      across(
        where(is.numeric),
        ~ ifelse(is.infinite(.x) | is.nan(.x), NA_real_, .x)
      )
    )
  
  # fivenum por index
  log_info("[{ctx}] Step 1b: Computing five-number summaries per index.")
  
  df_fivenum_index <- index_names %>%
    map(calc_fivenum, df_index_calculated) %>%
    map2(index_names, \(x, y) x %>% mutate(index_name = y, .before = 1)) %>%
    bind_rows()
  
  fivenum_path <- fs::path(PROCESSED_PATH, "fivenum_index.csv")
  write_csv(df_fivenum_index, fivenum_path)
  log_info("[{ctx}] fivenum_index written to: {fivenum_path}")
  
  df_index_calculated <- df_index_calculated %>%
    select(
      cod_IBGE,
      all_of(df_index_test$index_name)
    )
  
  # Step 2: log de excecoes
  log_info("[{ctx}] Step 2: Extracting last run exception logs (if any).")
  
  log_exception_path <- fs::path(PROCESSED_PATH, "log_exception_rules.csv")
  
  try({
    df_logs_indicator_calc <- "logs/indicator_calc.log" %>%
      read_lines() %>%
      as_tibble() %>%
      rename(log = value) %>%
      rowid_to_column()
    
    first_row <- df_logs_indicator_calc %>%
      filter(str_detect(log, "==== Starting indicator calculation pipeline ====")) %>%
      tail(1) %>%
      pull(rowid)
    
    df_logs_indicator_calc %>%
      filter(rowid >= first_row) %>%
      select(log) %>%
      write_csv(log_exception_path)
    
    log_info("[{ctx}] log_exception_rules written to: {log_exception_path}")
  }, silent = TRUE)
  
  # Step 3: salvar index_calculated
  index_calc_path <- fs::path(PROCESSED_PATH, "index_calculated.csv")
  
  df_index_calculated %>%
    write_csv(index_calc_path)
  
  log_info("[{ctx}] index_calculated written to: {index_calc_path}")
  
  # Step 4: comparacao oficial x calculado
  log_info("[{ctx}] Step 4: Building official vs calculated comparison.")
  
  df_index_calculated_renamed <- df_index_calculated %>%
    rename_with(
      .fn   = ~ str_c(., "_calculated"),
      .cols = df_index_test$index_name
    )
  
  df_index_official_full <- df_files %>%
    filter(
      file_type  == "Indicadores",
      file_name  == "Base Municipal",
      sheet_name %in% c("Atendimento", "Estruturais e Operacionais", "Qualidade")
    ) %>%
    pull(data)
  
  df_index_official <- df_index_official_full[[1]] %>%
    inner_join(df_index_official_full[[2]]) %>%
    inner_join(df_index_official_full[[3]]) %>%
    select(cod_IBGE, all_of(df_index_test$index_name)) %>%
    mutate(
      across(
        all_of(df_index_test$index_name),
        as.numeric
      )
    ) %>%
    rename_with(
      .fn   = ~ str_c(., "_official"),
      .cols = df_index_test$index_name
    )
  
  df_index_difference <- inner_join(
    df_index_official,
    df_index_calculated_renamed,
    by = "cod_IBGE"
  ) %>%
    pivot_longer(cols = -cod_IBGE) %>%
    separate_wider_delim(
      cols  = name,
      delim = "_",
      names = c("index", "source")
    ) %>%
    pivot_wider(
      id_cols     = c(cod_IBGE, index),
      names_from  = source,
      values_from = value
    ) %>%
    mutate(
      # dif = round(official, 2) - round(calculated, 2)
      dif = official - calculated
    )
  
  # Step 4.1: tabela validacao cod_IBGE (dif + flag)
  df_index_validation_cod_ibge <- df_index_difference %>%
    mutate(
      dif_class = if_else(
        abs(dif) >= 0.01, 1, 0
      )
    )
  
  val_path <- fs::path(PROCESSED_PATH, "index_validation_cod_ibge.csv")
  write_csv(df_index_validation_cod_ibge, val_path)
  log_info("[{ctx}] index_validation_cod_ibge written to: {val_path}")
  
  # Step 5: stats para calculado e diferença - para boxplots
  log_info("[{ctx}] Step 5: Building boxplot stats for indices and differences.")
  
  df_stats_boxplot <- df_index_difference %>%
    rename(name = index) %>%
    summarise(
      low    = min(calculated, na.rm = TRUE),
      q1     = quantile(calculated, 0.25, na.rm = TRUE),
      median = median(calculated, na.rm = TRUE),
      q3     = quantile(calculated, 0.75, na.rm = TRUE),
      high   = max(calculated, na.rm = TRUE),
      n      = sum(!is.na(calculated)),
      .by    = name
    )
  
  stats_boxplot_path <- fs::path(PROCESSED_PATH, "stats_boxplot.csv")
  write_csv(df_stats_boxplot, stats_boxplot_path)
  log_info("[{ctx}] stats_boxplot written to: {stats_boxplot_path}")
  
  df_difference_stats_boxplot <- df_index_difference %>%
    rename(name = index) %>%
    summarise(
      low    = min(dif, na.rm = TRUE),
      q1     = quantile(dif, 0.25, na.rm = TRUE),
      median = median(dif, na.rm = TRUE),
      q3     = quantile(dif, 0.75, na.rm = TRUE),
      high   = max(dif, na.rm = TRUE),
      n      = sum(!is.na(dif)),
      .by    = name
    )
  
  diff_stats_path <- fs::path(PROCESSED_PATH, "difference_stats_boxplot.csv")
  write_csv(df_difference_stats_boxplot, diff_stats_path)
  log_info("[{ctx}] difference_stats_boxplot written to: {diff_stats_path}")
  
  log_info("[{ctx}] ==== Indicator calculation pipeline finished successfully ====")
  
  invisible(
    list(
      df_index_calculated          = df_index_calculated,
      df_index_difference          = df_index_difference,
      df_index_validation_cod_ibge = df_index_validation_cod_ibge,
      df_fivenum_index             = df_fivenum_index,
      df_stats_boxplot             = df_stats_boxplot,
      df_difference_stats_boxplot  = df_difference_stats_boxplot
    )
  )
}


# ============================================================
# Main: Full pipeline (prepare + compute)
# ============================================================
prepare_and_compute_sinisa <- function(context = "SINISA_PIPELINE") {
  
  ctx <- context
  log_info("[{ctx}] ==== Starting SINISA preparation + index calculation pipeline ====")
  
  df_files <- build_df_files(context = ctx)
  
  # dataset_id 8: Gestão Técnica Água - Base Municipal (precisa melhor isso)
  df_gestao_tecnica <- df_files$data[[8]]
  gestao_path <- fs::path(PROCESSED_PATH, "gestao_tecnica_agua.csv")
  write_csv(df_gestao_tecnica, gestao_path)
  log_info("[{ctx}] gestao_tecnica_agua written to: {gestao_path}")
  
  df_description_cols <- build_description_cols(df_files, context = ctx)
  
  meta <- build_index_metadata(df_files, df_description_cols, context = ctx)
  df_index_municipal <- meta$df_index_municipal
  df_index_test      <- meta$df_index_test
  
  build_statistics_tables(df_gestao_tecnica, context = ctx)
  
  indicator_outputs <- run_indicator_pipeline(
    df_files     = df_files,
    df_index_test = df_index_test,
    context      = "IDB_INDEX_V1"
  )
  
  log_info("[{ctx}] ==== SINISA preparation + index calculation pipeline finished successfully ====")
  
  invisible(
    list(
      df_files            = df_files,
      df_description_cols = df_description_cols,
      df_index_municipal  = df_index_municipal,
      df_index_test       = df_index_test,
      df_gestao_tecnica   = df_gestao_tecnica
    ) |>
      c(indicator_outputs)
  )
}


# ============================================================
# Run
# ============================================================
log_info("[SINISA_PIPELINE] Script executed directly. Running prepare_and_compute_sinisa().")
prepare_and_compute_sinisa()
