collateDiagnoses <- function(conn,
                             hes_apc_tables,
                             base_directory_path,
                             data_dir) {

  apc_cols <- c("token_person_id",
                "epikey",
                "admidate",
                "epistart",
                "epiend",
                "disdate",
                "epiorder",
                "startage")

  diag_cols <- c(paste0("diag_", formatC(1:20, width = 2, flag = "0", format = "d")), "cause")

  sql_query <- paste("SELECT",
                     paste0(apc_cols, ",", collapse = " "),
                     rep(substring(hes_apc_tables, 21), each = length(diag_cols)), "AS reporting_year_start,",
                     rep(1:length(diag_cols), length(hes_apc_tables)), "AS diagnostic_position,",
                     "SUBSTRING(", diag_cols, ", 1, 4) AS diagnosis,",
                     "SUBSTRING(", diag_cols, ", 1, 3) AS diagnosis_3char",
                     "FROM", rep(hes_apc_tables, each = length(diag_cols)), "WHERE epistat = '3' AND",
                     rep(diag_cols, length(hes_apc_tables)), "IS NOT NULL AND",
                     rep(diag_cols, length(hes_apc_tables)), "!= ''",
                     collapse = " UNION ALL ")

  collated_diagnoses <- executeQuery(conn, sql_query)

  write_result <- writeParquetMultiFile(collated_diagnoses,
                                        path = paste0(base_directory_path,
                                                      data_dir),
                                        partitioning = "reporting_year_start")

  return(write_result)
}
