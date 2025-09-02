writeEchoccsCohortAsCsv <- function(echoccs_data_directory_path,
                                    echoccs_cohort_name) {

  # If this line doesn't work, use the commented out code (immediately below) instead.
  echoccs_cohort_all_data <- read_parquet(paste0(echoccs_data_directory_path,
                                                 echoccs_cohort_name,
                                                 "/",
                                                 echoccs_cohort_name,
                                                 ".parquet"))


  fwrite(echoccs_cohort_all_data,
         file = paste0(echoccs_data_directory_path,
                       echoccs_cohort_name,
                       "/",
                       echoccs_cohort_name,
                       ".csv"))

  metadata <- data.table(col_names = colnames(echoccs_cohort_all_data),
                         observations = c(nrow(echoccs_cohort_all_data),
                                          rep(NA, ncol(echoccs_cohort_all_data) - 1)))

  fwrite(metadata,
         file = paste0(echoccs_data_directory_path,
                       echoccs_cohort_name,
                       "/",
                       echoccs_cohort_name,
                       "_metadata.csv"))
}
