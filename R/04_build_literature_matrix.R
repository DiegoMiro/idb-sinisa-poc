# ============================================================
# 04_build_literature_matrix.R
# ------------------------------------------------------------
# Purpose:
#   - Build a curated literature matrix for the IDB/SINISA project
#   - Add internal IDs and "read" status flag
#   - Export a clean CSV for use in Quarto slides / tables
#   - Log the whole process to logs/literature_matrix.log
#
# ============================================================

suppressPackageStartupMessages({
  library(tibble)
  library(dplyr)
  library(stringr)
  library(readr)
  library(fs)
  library(logger)
})

# -------------------------------
# Paths
# -------------------------------
PROCESSED_PATH <- "data/processed"

if (!fs::dir_exists(PROCESSED_PATH)) fs::dir_create(PROCESSED_PATH)

# -------------------------------
# Logging setup
# -------------------------------
log_dir  <- "logs"
log_file <- fs::path(log_dir, "literature_matrix.log")

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
#' Build and export literature matrix
#'
#' @param context Logging context label for this step
#'
#' @return Invisibly returns a list with:
#'   - df_literature_matrix (full)
#'   - tbl_literature_matrix (export subset)
#'
build_literature_matrix <- function(context = "LITERATURE_MATRIX") {
  
  ctx <- context
  log_info("[{ctx}] ==== Building literature matrix ====")
  
  # --------------------------------------------------------
  # 1) Base tibble
  # --------------------------------------------------------
  df_literature_matrix <- tribble(
    ~authors, ~year, ~title, ~doc_type, ~source,
    ~region, ~scale, ~sector, ~data_type,
    ~period, ~methods, ~indicators,
    ~research_question, ~main_findings,
    ~policy_implications, ~limitations,
    ~project_relevance, ~project_use,
    ~related_studies, ~language, ~link,
    ~access, ~notes
  )
  
  # SINISA official report (Brazil – Ministry of Cities)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Brazil – Ministry of Cities",
      year = 2024,
      title = "SINISA – National Sanitation Information System: Water Supply Report 2024",
      doc_type = "Institutional report",
      source = "Brazil – Ministry of Cities",
      region = "Brazil",
      scale = "National / municipal coverage",
      sector = "Water supply",
      data_type = "Administrative and sectoral data reported by utilities",
      period = "Reference year 2023/2024",
      methods = "Descriptive statistics, indicator system, sector reporting methodology",
      indicators = "Coverage, service continuity, production, distribution, operational and service indicators",
      research_question = "What is the national situation of water supply services in Brazil according to the new SINISA framework?",
      main_findings = "Provides official national diagnostic of water supply services, with standardized indicators and methodological alignment to the new national sanitation information framework.",
      policy_implications = "Key evidence base for regulatory alignment, performance monitoring, and sector planning in Brazil.",
      limitations = "Administrative data dependent on quality and completeness of provider reporting.",
      project_relevance = "High",
      project_use = "Primary national data source for Brazil and conceptual basis for indicator construction and harmonization.",
      related_studies = "Connects with IDB, OECD and World Bank studies on sector performance and governance.",
      language = "PT",
      link = "https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/sinisa/resultados-sinisa/RELATORIO_SINISA_ABASTECIMENTO_DE_AGUA_2024.pdf",
      access = "Open access",
      notes = NA_character_
    )
  
  # Digital Journey (IDB, 2022)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Vásquez, W. et al.",
      year = 2022,
      title = "The Digital Journey of Water and Sanitation Utilities in Latin America and the Caribbean: What Is at Stake and How to Begin",
      doc_type = "Institutional report",
      source = "Inter-American Development Bank (IDB)",
      region = "Latin America and the Caribbean",
      scale = "Regional / utilities",
      sector = "Water and sanitation",
      data_type = "Institutional survey and case-based evidence",
      period = "-",
      methods = "Institutional diagnostics and policy analysis",
      indicators = "Digital maturity, institutional capacity, technology adoption",
      research_question = "Why does digital transformation matter for water and sanitation utilities in LAC, and how can they start this journey?",
      main_findings = "Maps current digital maturity of utilities, identifies common barriers and key enablers for digital transformation.",
      policy_implications = "Highlights how regulation, governance, and incentives can support or hinder digitalization and performance improvements.",
      limitations = "Primarily descriptive and strategic; does not quantify causal impacts.",
      project_relevance = "High",
      project_use = "Context for institutional capacity and modernization when interpreting performance and regulatory data.",
      related_studies = "Complements efficiency and regulation studies by adding a digital transformation dimension.",
      language = "EN",
      link = "https://publications.iadb.org/en/digital-journey-water-and-sanitation-utilities-latin-america-and-caribbean-what-stake-and-how-begin",
      access = "Open access",
      notes = NA_character_
    )
  
  # Vidal (2021, IDB)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Vidal, A. G.",
      year = 2021,
      title = "Water and Sanitation Services in Latin America: Access and Quality Outlook",
      doc_type = "Report",
      source = "Inter-American Development Bank (IDB)",
      region = "Latin America and the Caribbean",
      scale = "Regional",
      sector = "Water and sanitation",
      data_type = "Regional databases and perception surveys (e.g., LAPOP)",
      period = "2000s onwards",
      methods = "Comparative descriptive statistics",
      indicators = "Access, service quality, continuity, user perceptions",
      research_question = "What is the current situation of access and quality of water and sanitation services in LAC, and how does it vary across countries and population groups?",
      main_findings = "Shows large disparities in access and quality, highlighting vulnerable populations and geographic inequalities.",
      policy_implications = "Provides evidence to justify prioritizing investments, regulatory action, and targeted programs to close gaps.",
      limitations = "Limited focus on regulatory mechanisms and institutional determinants.",
      project_relevance = "Medium",
      project_use = "Contextual background for the diagnosis and problem statement in the BID project.",
      related_studies = "Complements regulation-focused studies by emphasizing final service outcomes for users.",
      language = "EN",
      link = "https://publications.iadb.org/en/water-and-sanitation-services-latin-america-access-and-quality-outlook",
      access = "Open access",
      notes = NA_character_
    )
  
  # Arias (2019, IDB)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Arias, L.",
      year = 2019,
      title = "The Regulation of Public Utilities of the Future in Latin America and the Caribbean: Water and Sanitation Sector",
      doc_type = "Institutional report",
      source = "Inter-American Development Bank (IDB)",
      region = "Latin America and the Caribbean",
      scale = "Regional",
      sector = "Water and sanitation",
      data_type = "Document and legal review; country cases",
      period = "-",
      methods = "Policy and institutional analysis",
      indicators = "Regulatory models, governance, incentives, sector performance dimensions",
      research_question = "What are the main regulatory challenges and future trends for water and sanitation utilities in LAC?",
      main_findings = "Maps existing regulatory arrangements, highlights structural challenges and outlines future directions for utility regulation.",
      policy_implications = "Provides a conceptual and institutional framework to guide regulatory reforms and performance-oriented regulation.",
      limitations = "Does not provide quantitative impact estimates or econometric analysis.",
      project_relevance = "High",
      project_use = "Conceptual backbone for the regulatory context section of the BID project report.",
      related_studies = "Complements quantitative performance and efficiency studies by adding a forward-looking regulatory perspective.",
      language = "EN",
      link = "https://publications.iadb.org/en/regulation-public-utilities-future-latin-america-and-caribbean-water-and-sanitation-sector",
      access = "Open access",
      notes = NA_character_
    )
  
  # Andrés et al. (2013, World Bank)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Andrés, L. A. et al.",
      year = 2013,
      title = "Uncovering the Drivers of Utility Performance in Latin America and the Caribbean",
      doc_type = "Research report / book",
      source = "World Bank",
      region = "Latin America and the Caribbean",
      scale = "Utilities",
      sector = "Infrastructure (including water, energy, telecom)",
      data_type = "Multi-sector utility panel data",
      period = "Varies by sector and country",
      methods = "Panel econometrics and institutional analysis",
      indicators = "Operational and financial performance, regulation, governance, private participation",
      research_question = "What factors drive the performance of infrastructure utilities in LAC?",
      main_findings = "Shows that regulation, governance, and institutional design are key determinants of utility performance across sectors.",
      policy_implications = "Provides a strong empirical basis for performance-oriented regulatory and governance reforms.",
      limitations = "Covers multiple sectors, so water and sanitation are one part of a broader analysis.",
      project_relevance = "High",
      project_use = "Reference for structuring the analysis of performance drivers in the BID project.",
      related_studies = "Links to BID and World Bank performance and regulation literature for the water sector.",
      language = "EN",
      link = "https://openknowledge.worldbank.org/entities/publication/10a77f66-8a31-5654-95b4-04ea55e432b4",
      access = "Open access",
      notes = NA_character_
    )
  
  # Carvalho (2023)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Carvalho, A. E. C.",
      year = 2023,
      title = "The impact of regulation on the Brazilian water and sewerage companies' levels of efficiency",
      doc_type = "Journal article",
      source = "Socio-Economic Planning Sciences",
      region = "Brazil",
      scale = "Utilities",
      sector = "Water and sewerage",
      data_type = "Administrative data from Brazilian utilities",
      period = "Panel (selected years)",
      methods = "DEA/SFA with panel econometrics",
      indicators = "Operational and financial efficiency",
      research_question = "To what extent does the regulatory environment affect efficiency levels of Brazilian water and sewerage companies?",
      main_findings = "Provides evidence that specific regulatory characteristics have a significant impact on utilities' efficiency.",
      policy_implications = "Direct empirical support for regulatory design and reform in the Brazilian water and sanitation sector.",
      limitations = "Restricted to a subset of companies and time periods; regulatory variables rely on proxies.",
      project_relevance = "High",
      project_use = "Empirical benchmark for Brazil when interpreting the integrated database and econometric results.",
      related_studies = "Complements regional studies on utility performance in Latin America.",
      language = "EN",
      link = "https://www.sciencedirect.com/science/article/abs/pii/S003801212300037X",
      access = "Journal",
      notes = NA_character_
    )
  
  # Ferro (2011)
  df_literature_matrix <- df_literature_matrix %>%
    add_row(
      authors = "Ferro, G. A.",
      year = 2011,
      title = "Regulation and performance: A production frontier estimate for the Latin American water and sanitation sector",
      doc_type = "Journal article",
      source = "Utilities Policy",
      region = "Latin America",
      scale = "Utilities",
      sector = "Water and sanitation",
      data_type = "Utility panel; secondary sectoral data",
      period = "Approx. 1990–2008",
      methods = "DEA/SFA and regression",
      indicators = "Efficiency, operational performance, coverage",
      research_question = "How do regulatory and institutional arrangements influence the efficiency of water and sanitation utilities in Latin America?",
      main_findings = "Finds significant associations between institutional/regulatory factors and utility efficiency across countries.",
      policy_implications = "Supports the use of benchmarking and efficiency analysis as tools for regulatory incentives.",
      limitations = "Data gaps and methodological differences across countries; focused on a subset of utilities.",
      project_relevance = "High",
      project_use = "Methodological reference for efficiency and benchmarking approaches in the integrated dataset.",
      related_studies = "Connects to World Bank and regional efficiency benchmarking literature.",
      language = "EN",
      link = "https://www.sciencedirect.com/science/article/abs/pii/S0957178711000579",
      access = "Journal (subscription or institutional access)",
      notes = NA_character_
    )
  
  # --------------------------------------------------------
  # 2) Add id_ref + read flag
  # --------------------------------------------------------
  df_literature_matrix <- df_literature_matrix %>%
    mutate(
      id_ref = str_c("LIT_", str_pad(row_number(), width = 3, pad = "0")),
      .before = 1
    ) %>%
    mutate(
      read = case_when(
        title %in% c(
          "SINISA – National Sanitation Information System: Water Supply Report 2024",
          "Water and Sanitation Services in Latin America: Access and Quality Outlook",
          "Uncovering the Drivers of Utility Performance in Latin America and the Caribbean"
        ) ~ "yes",
        TRUE ~ "no"
      ),
      .after = 1
    )
  
  log_info("[{ctx}] Literature matrix built with {nrow(df_literature_matrix)} reference(s).")
  
  # --------------------------------------------------------
  # 3) Export subset for analysis / tables
  # --------------------------------------------------------
  tbl_literature_matrix <- df_literature_matrix %>%
    select(
      read, id_ref, title, link, year, source, authors,
      region, doc_type, access, sector, data_type, methods, indicators,
      research_question, main_findings, policy_implications,
      limitations, project_use, related_studies
    )
  
  out_path <- fs::path(PROCESSED_PATH, "literature_matrix.csv")
  write_csv(tbl_literature_matrix, out_path)
  
  log_info("[{ctx}] literature_matrix.csv written to: {out_path}")
  log_info("[{ctx}] ==== Literature matrix build finished successfully ====")
  
  invisible(
    list(
      df_literature_matrix = df_literature_matrix,
      tbl_literature_matrix = tbl_literature_matrix
    )
  )
}


# ============================================================
# Run
# ============================================================
log_info("[LITERATURE_MATRIX] Script executed directly. Running build_literature_matrix().")
build_literature_matrix()
