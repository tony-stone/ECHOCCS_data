collateCancerDiagnoses <- function(conn,
                                   echoccs_data_directory_path,
                                   diagnoses_filepath,
                                   table_name) {

  # conn <- duckdb_conn
  # table_name <- cancer_diagnoses_name

  # Included codes as per CRUK Children's cancers incidence statistics 2017-2019
  # Includes all malignant tumours (ICD10 Chapter C codes) and all non-malignant
  # brain, and other CNS and intracranial tumours (ICD10 Chapter D codes).

  cancer_diag_groupings <- rbind(data.table(diagnosis_3char = c(paste0("C", sprintf("%02d", c(0:74, 76:97))),
                                                                paste0("D", c(32, 33, 42, 43))),
                                            diagnosis_4char = as.character(NA)),
                                 data.table(diagnosis_3char = as.character(NA),
                                            diagnosis_4char = c(paste0("C", 750:759),
                                                                paste0("D", c(352:354, 443:445)))))

  cancer_diag_groupings[, cancer_group := "Other"]
  cancer_diag_groupings[diagnosis_3char %in% c(paste0("C", c(70:72)),
                                               paste0("D", c(32, 33, 42, 43))) |
                          diagnosis_4char %in% c(paste0("C", c(752:754)),
                                                 paste0("D", c(352:354, 443:445))),
                        cancer_group := "Brain / CNS"]
  cancer_diag_groupings[diagnosis_3char %in% paste0("C", c(81:89, 96)),
                        cancer_group := "Lymphoma"]
  cancer_diag_groupings[diagnosis_3char %in% paste0("C", c(90:95)),
                        cancer_group := "Leukaemia"]

  fwrite(cancer_diag_groupings,
         "data-out/2025-11-06_cancer_diag_groupings.csv")

  temp_file_path <- writeTempParquetFile(cancer_diag_groupings)

  sql_create_groupings <- paste0("DROP TABLE IF EXISTS cancer_diag_groupings;",
                                 "CREATE TABLE cancer_diag_groupings AS (",
                                 "SELECT * FROM read_parquet('",
                                 temp_file_path,
                                 "'));")
  res <- executeQuery(conn,
                      sql_query = sql_create_groupings,
                      query_type = "SEND")

  unlink(temp_file_path)

  date_fields <- c("admidate",
                   "epistart",
                   "epiend",
                   "disdate")

  sql_diagnoses_temp <-paste("DROP TABLE IF EXISTS cancer_diagnoses_temp;",
                             "CREATE TABLE cancer_diagnoses_temp AS (",
                             "SELECT",
                             "groups.cancer_group,",
                             "diags.token_person_id,",
                             "diags.reporting_year_start,",
                             "diags.epikey,",
                             "diags.diagnostic_position,",
                             "diags.epiorder,",
                             paste0("diags.",
                                    date_fields,
                                    collapse = ", "),
                             "FROM cancer_diag_groupings AS groups",
                             "INNER JOIN",
                             paste0("read_parquet('",
                                    diagnoses_filepath,
                                    "/*/*.parquet', ",
                                    "hive_partitioning = true)"),
                             "AS diags ON ((",
                             "groups.diagnosis_3char = diags.diagnosis_3char OR",
                             "groups.diagnosis_4char = diags.diagnosis",
                             ") AND diags.diagnostic_position = 1));")

  res <- executeQuery(conn,
                      sql_query = sql_diagnoses_temp,
                      query_type = "SEND")

  sql_diagnoses <- paste("DROP TABLE IF EXISTS cancer_diagnoses;",
                         "CREATE TABLE cancer_diagnoses AS (",
                         "SELECT",
                         "cancer_group,",
                         "token_person_id,",
                         "reporting_year_start,",
                         "epikey,",
                         "diagnostic_position,",
                         "CAST(epiorder AS INT) AS epiorder,",
                         paste0("CAST(CASE WHEN ",
                                date_fields,
                                " IN ('', '1800-01-01', '1801-01-01') THEN NULL ",
                                "ELSE ", date_fields, " END ",
                                "AS DATE) AS ",
                                date_fields,
                                ", ",
                                collapse = ""),
                         "LEAST(",
                         paste0("CAST(",
                                "CASE WHEN ",
                                date_fields,
                                " IN ('', '1800-01-01', '1801-01-01') THEN NULL ",
                                "ELSE ", date_fields, " END ",
                                "AS DATE)",
                                collapse = ", "),
                         ") AS min_date",
                         "FROM cancer_diagnoses_temp);")

  res <- executeQuery(conn,
                      sql_query = sql_diagnoses,
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE cancer_diagnoses_temp;",
                                        "DROP TABLE cancer_diag_groupings;"),
                      query_type = "SEND")

  missing_all_dates <- executeQuery(conn,
                                    paste("SELECT COUNT(*) AS missing_all_dates",
                                          "FROM cancer_diagnoses",
                                          "WHERE min_date IS NULL"))

  if(missing_all_dates > 0) stop("All dates missing.")

  sql_first_diagnosis_temp <- paste("DROP TABLE IF EXISTS first_diagnoses_temp;",
                                    "CREATE TABLE first_diagnoses_temp AS (",
                                    "SELECT token_person_id,",
                                    "row_number() OVER (PARTITION BY token_person_id",
                                    "ORDER BY min_date) AS diagnosis_order,",
                                    "cancer_group,",
                                    "CASE WHEN earliest_admidate > min_date THEN min_date",
                                    "ELSE earliest_admidate END AS earliest_admission,",
                                    "CASE WHEN admidate_missing = 1 OR",
                                    "earliest_admidate > min_date THEN 1 ELSE 0 END AS",
                                    "earliest_admission_problematic FROM (",
                                    "SELECT token_person_id, cancer_group,",
                                    "MIN(admidate) AS earliest_admidate,",
                                    "MAX(CASE WHEN admidate IS NULL THEN 1 ELSE 0 END) AS admidate_missing,",
                                    "MIN(min_date) AS min_date",
                                    "FROM cancer_diagnoses",
                                    "GROUP BY token_person_id, cancer_group) AS sq);")

  res <- executeQuery(conn,
                      sql_query = sql_first_diagnosis_temp,
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE cancer_diagnoses;",
                      query_type = "SEND")

  sql_first_diagnosis <- paste("DROP TABLE IF EXISTS first_diagnoses;",
                               "CREATE TABLE first_diagnoses AS (",
                               "PIVOT first_diagnoses_temp",
                               "ON diagnosis_order",
                               "USING FIRST(cancer_group) AS group,",
                               "FIRST(earliest_admission) AS earliest_diagnosis,",
                               "FIRST(earliest_admission_problematic) AS earliest_diagnosis_problematic,",
                               ");")

  res <- executeQuery(conn,
                      sql_query = sql_first_diagnosis,
                      query_type = "SEND")

  col_names <- c("group",
                 "earliest_diagnosis",
                 "earliest_diagnosis_problematic")

  sql_first_diagnosis_alter <- paste0("ALTER TABLE first_diagnoses RENAME COLUMN \"",
                                      rep(1:4, each = length(col_names)),
                                      "_",
                                      rep(col_names, 4),
                                      "\" TO cancer_",
                                      rep(col_names, 4),
                                      "_",
                                      rep(1:4, each = length(col_names)),
                                      ";",
                                      collapse = " ")

  res <- executeQuery(conn,
                      sql_query = sql_first_diagnosis_alter,
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE first_diagnoses_temp;",
                      query_type = "SEND")

  cancer_diagnoses_data <- executeQuery(conn,
                                        sql_query = "SELECT * FROM first_diagnoses;")

  write_result <- writeParquetFile(cancer_diagnoses_data,
                                   dir_path = paste0(echoccs_data_directory_path,
                                                     table_name),
                                   filename = table_name)

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE first_diagnoses;",
                      query_type = "SEND")
  rm(cancer_diagnoses_data)
  gc()

  return(write_result)
}
