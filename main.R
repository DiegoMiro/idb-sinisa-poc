# ============================================================
# main.R
# ------------------------------------------------------------
# Run Ppipeline Steps in Order:
#   R/01_download_unzip_sinisa.R
#   R/02_prepare_and_compute_sinisa.R
#   R/03_build_ibge_municipal_table.R
#   R/03_compare_sinisa_ibge.R
#   R/04_build_literature_matrix.R
#
# ============================================================

source("R/01_download_unzip_sinisa.R")
source("R/02_prepare_and_compute_sinisa.R")
source("R/03_build_ibge_municipal_table.R")
source("R/03_compare_sinisa_ibge.R")
source("R/04_build_literature_matrix.R")
