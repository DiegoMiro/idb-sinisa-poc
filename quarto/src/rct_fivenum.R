rct_fivenum <- function(tbl_fivenum) {
  
  library(readr)
  library(dplyr)
  library(reactable)
  library(htmltools)
  library(scales)
  
  # Referência Brasil por index_name (para comparação)
  brasil_ref <- tbl_fivenum %>%
    filter(type == "Brasil") %>%
    transmute(
      n_br      = n,
      na_br     = na,
      v0_br     = v0,
      g100_br   = g100,
      low_br    = low,
      q1_br     = q1,
      median_br = median,
      q3_br     = q3,
      high_br   = high
    )
  
  # Junta a referência de Brasil em todas as linhas do mesmo index_name
  tbl <- tbl_fivenum %>%
    bind_cols(brasil_ref)
  
  # ---- Helpers para células ----
  
  # Contagens: valor + % em relação ao Brasil + barra azul
  count_cell_factory <- function(col, col_br) {
    function(value, index) {
      brasil_val <- tbl[[col_br]][index]
      type_val   <- tbl$type[index]
      
      pct <- if (is.na(brasil_val) || brasil_val == 0 || is.na(value)) {
        NA_real_
      } else {
        value / brasil_val
      }
      
      pct_label <- if (is.na(pct)) {
        "–"
      } else {
        scales::percent(pct, accuracy = 0.1, decimal.mark = ",")
      }
      
      bar_width <- if (is.na(pct)) 0 else max(0, min(1, pct))
      
      
      output <- if (type_val == "Brasil") {
        div(
          span(format(value, big.mark = ".", decimal.mark = ","))
        )
      } else {
        div(
          # Valor principal
          span(format(value, big.mark = ".", decimal.mark = ",")),
          tags$br(),
          # Texto menor: % em relação ao Brasil
          span(
            style = "font-size: 0.85rem; color: #555;",
            pct_label
          ),
          # Barra de progresso azul
          div(
            style = "
        margin-top: 4px;
        height: 4px;
        background-color: #d5e5fb;
        border-radius: 999px;
        overflow: hidden;
        display:flex;
        justify-content:flex-start;
        flex-direction:row-reverse;
        ",
            div(
              style = paste0(
                "height: 100%; width:", bar_width * 100, "%;",
                "background-color:#2f80ed; border-radius: 999px;"
              )
            )
          )
        )
      }
      output
    }
  }
  
  # Quantis: valor + desvio vs Brasil (↑/↓) + barra verde/vermelha
  quant_cell_factory <- function(col, col_br) {
    diff_vec <- tbl[[col]] - tbl[[col_br]]
    max_diff <- suppressWarnings(
      max(abs(diff_vec[tbl$type != "Brasil"]), na.rm = TRUE)
    )
    
    function(value, index) {
      brasil_val <- tbl[[col_br]][index]
      type_val   <- tbl$type[index]
      diff       <- diff_vec[index]
      
      diff <- if (is.na(value) || is.na(brasil_val)) {
        NA_real_
      } else {
        round(value - brasil_val, 2)
      }
      
      # arrow <- if (is.na(diff) || diff == 0) {
      #   "–"
      # } else if (diff > 0) {
      #   "\u25B2" # ▲
      # } else {
      #   "\u25BC" # ▼
      # }
      
      color <- if (is.na(diff) || diff == 0) {
        "#555555"
      } else if (diff > 0) {
        "#2e7d32" # verde
      } else {
        "#c62828" # vermelho
      }
      
      
      
      diff_label <- if (is.na(diff)) {
        "sem comparação"
      } else {
        sinal <- if (diff > 0) "+" else if (diff < 0) "−" else ""
        paste0(
          sinal,
          format(round(abs(diff), 1), big.mark = ".", decimal.mark = ",")
        )
      }
      
      # Percentual do desvio em relação ao valor Brasil (para a barra)
      pct <- if (is.na(brasil_val) || brasil_val == 0 || is.na(diff)) {
        NA_real_
      } else {
        diff / brasil_val
      }
      
      bar_width <- if (is.na(pct)) 0 else max(0, min(1, abs(pct)))
      
      bar_color <- if (is.na(diff) || diff == 0) "#9e9e9e"
      else if (diff > 0) "#2e7d32" else "#c62828"
      
      bar_fill <- if (is.na(diff) || diff == 0) "#ebebeb"
      else if (diff > 0) "#d5e5d6" else "#f3d4d4"
      
      output <- if (type_val == "Brasil") {
        div(
          span(format(round(value, 1), big.mark = ".", decimal.mark = ","))
        )
      } else {
        div(
          # Valor principal
          span(format(round(value, 1), big.mark = ".", decimal.mark = ",")),
          tags$br(),
          # Texto menor: desvio vs Brasil com seta
          span(
            style = paste0("font-size: 0.85rem; color:", color, ";"),
            diff_label
          ),
          # Barra de progresso verde/vermelha
          div(
            style = paste0("
        margin-top: 4px;
        height: 4px;
        background-color: ", bar_fill, ";
        border-radius:
        999px;
        overflow: hidden;
        display:flex;
        justify-content:flex-start;
        flex-direction:row-reverse;
        "),
            div(
              style = paste0(
                "height: 100%; width:", bar_width * 100, "%;",
                "background-color:", bar_color, "; border-radius: 999px;"
              )
            )
          )
        )
      }
      output
    }
  }
  
  # ---- Definição das colunas ----
  
  cols <- list(
    type = colDef(show = FALSE),
    n_br = colDef(show = FALSE),
    na_br = colDef(show = FALSE),
    v0_br = colDef(show = FALSE),
    g100_br = colDef(show = FALSE),
    low_br = colDef(show = FALSE),
    q1_br = colDef(show = FALSE),
    median_br = colDef(show = FALSE),
    q3_br = colDef(show = FALSE),
    high_br = colDef(show = FALSE),
    
    name = colDef(
      name = "Nome",
      sticky = "left",
      style = list(
        borderRight = "1px solid #e0e0e0"
      ),
      minWidth = 140
    ),
    
    # Contagens
    n = colDef(
      name = "Municípios",
      html = TRUE,
      align = "right",
      cell = count_cell_factory("n", "n_br")
    ),
    na = colDef(
      name = "Missing",
      html = TRUE,
      align = "right",
      cell = count_cell_factory("na", "na_br")
    ),
    v0 = colDef(
      name = "Index=0",
      html = TRUE,
      align = "right",
      cell = count_cell_factory("v0", "v0_br")
    ),
    g100 = colDef(
      name = "Index>100",
      html = TRUE,
      align = "right",
      style = list(
        borderRight = "1px solid #e0e0e0"
      ),
      cell = count_cell_factory("g100", "g100_br")
    ),
    
    # Quantis
    low = colDef(
      name = "Low",
      html = TRUE, align = "right",
      cell = quant_cell_factory("low", "low_br")
    ),
    q1 = colDef(
      name = "Q1",
      html = TRUE,
      align = "right",
      cell = quant_cell_factory("q1", "q1_br")
    ),
    median = colDef(
      name = "Median",
      html = TRUE,
      align = "right",
      cell = quant_cell_factory("median", "median_br")
    ),
    q3 = colDef(
      name = "Q3",
      html = TRUE,
      align = "right",
      cell = quant_cell_factory("q3", "q3_br")
    ),
    high = colDef(
      name = "High",
      html = TRUE,
      align = "right",
      cell = quant_cell_factory("high", "high_br")
    )
  )
  
  # ---- Tabela reactable final ----
  
  reactable(
    tbl,
    columns = cols,
    height = 450,
    highlight = TRUE,
    bordered = FALSE,
    sortable = FALSE,
    striped = FALSE,
    filterable = FALSE,
    pagination = FALSE,
    rowStyle = reactable::JS(
      "function(rowInfo) {
       if (!rowInfo) return {};
       var t = rowInfo.row.type;
       if (t === 'Macrorregião') {
         return {
           backgroundColor: '#f5f5f5'
         };
       }
       if (t === 'Brasil') {
         return {
           backgroundColor: '#e5e5e5',
           fontWeight: 'bold'
         };
       }
       return {};
     }"
    ),
    columnGroups = list(
      colGroup(name = "Counts", columns = c("n", "na", "v0", "g100")),
      colGroup(name = "Quantiles", columns = c("low", "q1", "median", "q3", "high"))
    ),
    theme = reactableTheme(
      cellPadding = "6px 8px",
      style = list(fontSize = "16px"),
      headerStyle = list(fontSize = "16px", whiteSpace = "nowrap"),
      tableStyle = list(
        paddingRight  = "8px",
        paddingBottom = "8px"
      )
    )
  )
  
  
}


# 
# tbl_fivenum %>%
#   filter(index_name == "IAG0001") %>%
#   rct_fivenum()

