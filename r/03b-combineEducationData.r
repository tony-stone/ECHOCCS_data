combineEducationData <- function(conn,
                                 echoccs_data_directory_path,
                                 education_data_directory,
                                 birth_cohort_name,
                                 education_data_combined_name) {

  # If Born <  September, YoB + 4 years is academic year of entry
  # If Born >= September, YoB + 5 years is academic year of entry

  # KS1 (Year 2) record in latest academic year within expected bound
  # KS2 (Year 6) record in latest academic year within expected bound
  # KS4 (Year 11) record in latest academic year within expected bound
  # KS5 (Year 13) record in latest academic year within expected bound


  # conn <- duckdb_conn

  education_data_path <- paste0(echoccs_data_directory_path,
                                education_data_directory)

  birth_cohort_filepath <- paste0(echoccs_data_directory_path,
                                  birth_cohort_name,
                                  "/",
                                  birth_cohort_name,
                                  ".parquet")



  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_cohort;",
                                        "CREATE TABLE birth_cohort AS (",
                                        "SELECT",
                                        "dfe_id,",
                                        "YEAR(min_dob) + CASE WHEN MONTH(min_dob) < 9 THEN 4 ELSE 5 END AS school_start_year",
                                        "FROM",
                                        paste0("read_parquet('",
                                               birth_cohort_filepath,
                                               "')"),
                                        ");"),
                      query_type = "SEND")

  ks_year_field <- "census_academic_year_start"
  ks_data_filename <- "ks1_census_data.parquet"
  year_offset <- "2"

  ks_year_field <- paste0("ks.", ks_year_field)
  ks1_census_data <- executeQuery(conn,
                                  sql_query = paste("SELECT * FROM (SELECT",
                                                    "ks.*,",
                                                    "ROW_NUMBER() OVER (PARTITION BY ks.dfe_id ORDER BY", ks_year_field, "DESC) AS row_order",
                                                    "FROM birth_cohort AS bc",
                                                    "INNER JOIN",
                                                    paste0("read_parquet('",
                                                           education_data_path, "/",
                                                           ks_data_filename,
                                                           "')"),
                                                    "AS ks ON",
                                                    "bc.dfe_id = ks.dfe_id AND",
                                                    "bc.school_start_year + ", year_offset, " >= ", ks_year_field,
                                                    ") AS q1",
                                                    "WHERE row_order = 1;"))

  setDT(ks1_census_data)
  ks1_census_data[, row_order := NULL]


  ks_year_field <- "ks1_academic_year_start"
  ks_data_filename <- "ks1_data.parquet"
  year_offset <- "2"

  ks_year_field <- paste0("ks.", ks_year_field)
  ks1_data <- executeQuery(conn,
                           sql_query = paste("SELECT * FROM (SELECT",
                                             "ks.*,",
                                             "ROW_NUMBER() OVER (PARTITION BY ks.dfe_id ORDER BY", ks_year_field, "DESC) AS row_order",
                                             "FROM birth_cohort AS bc",
                                             "INNER JOIN",
                                             paste0("read_parquet('",
                                                    education_data_path, "/",
                                                    ks_data_filename,
                                                    "')"),
                                             "AS ks ON",
                                             "bc.dfe_id = ks.dfe_id AND",
                                             "bc.school_start_year + ", year_offset, " >= ", ks_year_field,
                                             ") AS q1",
                                             "WHERE row_order = 1;"))

  setDT(ks1_data)
  ks1_data[, row_order := NULL]


  ks_year_field <- "ks2_academic_year_start"
  ks_data_filename <- "ks2_data.parquet"
  year_offset <- "6"

  ks_year_field <- paste0("ks.", ks_year_field)
  ks2_data <- executeQuery(conn,
                           sql_query = paste("SELECT * FROM (SELECT",
                                             "ks.*,",
                                             "ROW_NUMBER() OVER (PARTITION BY ks.dfe_id ORDER BY", ks_year_field, "DESC) AS row_order",
                                             "FROM birth_cohort AS bc",
                                             "INNER JOIN",
                                             paste0("read_parquet('",
                                                    education_data_path, "/",
                                                    ks_data_filename,
                                                    "')"),
                                             "AS ks ON",
                                             "bc.dfe_id = ks.dfe_id AND",
                                             "bc.school_start_year + ", year_offset, " >= ", ks_year_field,
                                             ") AS q1",
                                             "WHERE row_order = 1;"))

  setDT(ks2_data)
  ks2_data[, row_order := NULL]




  ks_year_field <- "ks4_academic_year_start"
  ks_data_filename <- "ks4_data.parquet"
  year_offset <- "11"

  ks_year_field <- paste0("ks.", ks_year_field)
  ks4_data <- executeQuery(conn,
                           sql_query = paste("SELECT * FROM (SELECT",
                                             "ks.*,",
                                             "ROW_NUMBER() OVER (PARTITION BY ks.dfe_id ORDER BY", ks_year_field, "DESC) AS row_order",
                                             "FROM birth_cohort AS bc",
                                             "INNER JOIN",
                                             paste0("read_parquet('",
                                                    education_data_path, "/",
                                                    ks_data_filename,
                                                    "')"),
                                             "AS ks ON",
                                             "bc.dfe_id = ks.dfe_id AND",
                                             "bc.school_start_year + ", year_offset, " >= ", ks_year_field,
                                             ") AS q1",
                                             "WHERE row_order = 1;"))

  setDT(ks4_data)
  ks4_data[, row_order := NULL]


  ks_year_field <- "ks5_academic_year_start"
  ks_data_filename <- "ks5_data.parquet"
  year_offset <- "13"

  ks_year_field <- paste0("ks.", ks_year_field)
  ks5_data <- executeQuery(conn,
                           sql_query = paste("SELECT * FROM (SELECT",
                                             "ks.*,",
                                             "ROW_NUMBER() OVER (PARTITION BY ks.dfe_id ORDER BY", ks_year_field, "DESC) AS row_order",
                                             "FROM birth_cohort AS bc",
                                             "INNER JOIN",
                                             paste0("read_parquet('",
                                                    education_data_path, "/",
                                                    ks_data_filename,
                                                    "')"),
                                             "AS ks ON",
                                             "bc.dfe_id = ks.dfe_id AND",
                                             "bc.school_start_year + ", year_offset, " >= ", ks_year_field,
                                             ") AS q1",
                                             "WHERE row_order = 1;"))

  setDT(ks5_data)
  ks5_data[, row_order := NULL]


  ks_data <- merge(ks1_census_data,
                   ks1_data,
                   by = "dfe_id",
                   all = TRUE)

  ks_data <- merge(ks_data,
                   ks2_data,
                   by = "dfe_id",
                   all = TRUE)

  ks_data <- merge(ks_data,
                   ks4_data,
                   by = "dfe_id",
                   all = TRUE)

  ks_data <- merge(ks_data,
                   ks5_data,
                   by = "dfe_id",
                   all = TRUE)

  write_ks_data <- writeParquetFile(ks_data,
                                    dir_path = education_data_path,
                                    filename = education_data_combined_name)

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE IF EXISTS birth_cohort;",
                      query_type = "SEND")
  rm(ks_data, ks1_census_data, ks1_data, ks2_data, ks4_data, ks5_data)
  gc()

  return(write_ks_data)
}


