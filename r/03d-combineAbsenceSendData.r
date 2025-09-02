combineAbsenceSendData <- function(conn,
                                   echoccs_data_directory_path,
                                   education_data_directory,
                                   birth_cohort_name,
                                   absence_send_combined_name) {

  # conn <- duckdb_conn

  education_data_path <- paste0(echoccs_data_directory_path,
                                education_data_directory,
                                "/")

  edu_census_filepath <- paste0(education_data_path,
                                edu_census_name,
                                ".parquet")

  edu_absence_filepath <- paste0(education_data_path,
                                 edu_absence_consolidated_name,
                                 ".parquet")

  birth_cohort_filepath <- paste0(echoccs_data_directory_path,
                                  birth_cohort_name,
                                  "/",
                                  birth_cohort_name,
                                  ".parquet")

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS edu_census;",
                                        "CREATE TABLE edu_census AS (",
                                        "SELECT",
                                        "CAST(CASE",
                                        "WHEN census_enrol_status = 'C' THEN 1",
                                        "WHEN census_enrol_status = 'M' THEN 2",
                                        "WHEN census_enrol_status = 'S' THEN 3",
                                        "WHEN census_enrol_status = 'F' THEN 4",
                                        "WHEN census_enrol_status = 'O' THEN 5",
                                        "WHEN census_enrol_status = 'G' THEN 6",
                                        "ELSE NULL",
                                        "END AS TINYINT) AS census_enrol_status_order,",
                                        "CAST(CASE",
                                        "WHEN census_SEN_status = 'N' THEN 0",
                                        "WHEN census_SEN_status = 'A' THEN 1",
                                        "WHEN census_SEN_status = 'P' THEN 2",
                                        "WHEN census_SEN_status = 'Q' THEN 3",
                                        "WHEN census_SEN_status = 'S' THEN 4",
                                        "WHEN census_SEN_status = 'K' THEN 5",
                                        "WHEN census_SEN_status = 'E' THEN 6",
                                        "ELSE NULL",
                                        "END AS TINYINT) AS census_SEN_status_order,",
                                        "CAST(SUBSTRING(census_academic_year, 1, 4) AS INTEGER) AS census_academic_year_start,",
                                        paste0(c("dfe_id",
                                                 "census_record_status",
                                                 "census_enrol_status",
                                                 "census_NC_year_actual",
                                                 "census_SEN_status",
                                                 "census_SEN_primary_type",
                                                 "census_SEN_secondary_type",
                                                 "census_special_provision_indicator"),
                                               collapse = ", "),
                                        "FROM",
                                        paste0("read_parquet('",
                                               edu_census_filepath,
                                               "')"),
                                        ");"),
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS edu_send;",
                                        "CREATE TABLE edu_send AS (",
                                        "SELECT",
                                        "ROW_NUMBER() OVER (PARTITION BY dfe_id, census_academic_year_start ORDER BY",
                                        "census_SEN_status_order DESC NULLS LAST,",
                                        "census_record_status ASC NULLS FIRST,",
                                        "census_enrol_status_order ASC NULLS LAST",
                                        ") As rec_order,",
                                        paste0(c("dfe_id",
                                                 "census_academic_year_start",
                                                 "census_record_status",
                                                 "census_enrol_status",
                                                 "census_NC_year_actual",
                                                 "census_SEN_status",
                                                 "census_SEN_primary_type",
                                                 "census_SEN_secondary_type",
                                                 "census_special_provision_indicator"),
                                               collapse = ", "),
                                        "FROM edu_census);"),
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE IF EXISTS edu_census;",
                      query_type = "SEND")
  gc()

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS edu_send_summary;",
                                        "CREATE TABLE edu_send_summary AS (",
                                        "SELECT",
                                        paste0("sen.",
                                               c("dfe_id",
                                                 "census_academic_year_start AS academic_year_start",
                                                 "census_NC_year_actual AS sen_NC_year_actual",
                                                 "census_SEN_status AS sen_status",
                                                 "census_SEN_primary_type AS sen_primary_type",
                                                 "census_SEN_secondary_type AS sen_secondary_type"),
                                               ", ",
                                               collapse = ""),
                                        "spi.any_special_provision AS sen_any_special_provision",
                                        "FROM (SELECT",
                                        paste0(c("dfe_id",
                                                 "census_academic_year_start",
                                                 "census_NC_year_actual",
                                                 "census_SEN_status",
                                                 "census_SEN_primary_type",
                                                 "census_SEN_secondary_type"),
                                               collapse = ", "),
                                        "FROM edu_send WHERE rec_order = 1) AS sen",
                                        "LEFT JOIN (",
                                        "SELECT",
                                        "dfe_id,",
                                        "census_academic_year_start,",
                                        "CAST(MAX(CASE WHEN census_special_provision_indicator IN ('1') THEN 1 ELSE 0 END) AS TINYINT) AS any_special_provision",
                                        "FROM edu_send",
                                        "GROUP BY dfe_id, census_academic_year_start) AS spi",
                                        "ON (sen.dfe_id = spi.dfe_id AND",
                                        "sen.census_academic_year_start = spi.census_academic_year_start));"),
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE IF EXISTS edu_send;",
                      query_type = "SEND")
  gc()


  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_cohort_sen;",
                                        "CREATE TABLE birth_cohort_sen AS (",
                                        "SELECT",
                                        "bc.dfe_id,",
                                        "CAST(YEAR(bc.min_dob) + CASE WHEN MONTH(bc.min_dob) < 9 THEN 4 ELSE 5 END AS INTEGER) AS school_start_year,",
                                        "sen.academic_year_start,",
                                        paste0("sen.sen_",
                                               c("NC_year_actual",
                                                 "status",
                                                 "primary_type",
                                                 "secondary_type",
                                                 "any_special_provision"),
                                               collapse = ", "),
                                        "FROM",
                                        paste0("read_parquet('",
                                               birth_cohort_filepath,
                                               "')"),
                                        "AS bc",
                                        "INNER JOIN edu_send_summary AS sen",
                                        "ON (bc.dfe_id = sen.dfe_id));"),
                      query_type = "SEND")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE IF EXISTS edu_send_summary;",
                      query_type = "SEND")
  gc()


  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_cohort_absence;",
                                        "CREATE TABLE birth_cohort_absence AS (",
                                        "SELECT",
                                        "bc.dfe_id,",
                                        "CAST(YEAR(bc.min_dob) + CASE WHEN MONTH(bc.min_dob) < 9 THEN 4 ELSE 5 END AS INTEGER) AS school_start_year,",
                                        paste0("abs.abs_",
                                               c("academic_year_start AS academic_year_start",
                                                 "records",
                                                 "annual_possible",
                                                 "annual_absent_total",
                                                 "summed_possible",
                                                 "summed_absent_total",
                                                 "summed_absent_illness_medical",
                                                 "summer6th_possible",
                                                 "summer6th_absent_total",
                                                 "summer6th_absent_illness_medical"),
                                               collapse = ", "),
                                        "FROM",
                                        paste0("read_parquet('",
                                               birth_cohort_filepath,
                                               "')"),
                                        "AS bc",
                                        "INNER JOIN",
                                        paste0("read_parquet('",
                                               edu_absence_filepath,
                                               "')"),
                                        "AS abs",
                                        "ON (bc.dfe_id = abs.dfe_id));"),
                      query_type = "SEND")


  sen_absence_data <- executeQuery(conn,
                      sql_query = paste("SELECT",
                                        "CASE WHEN sen.dfe_id IS NULL THEN abs.dfe_id ELSE sen.dfe_id END AS dfe_id,",
                                        "CASE WHEN sen.academic_year_start IS NULL THEN abs.academic_year_start ELSE sen.academic_year_start END AS senabs_academic_year_start,",
                                        "CASE WHEN sen.school_start_year IS NULL THEN abs.school_start_year ELSE sen.school_start_year END AS senabs_school_start_year,",
                                        paste0("sen.sen_",
                                               c("NC_year_actual",
                                                 "status",
                                                 "primary_type",
                                                 "secondary_type",
                                                 "any_special_provision"),
                                               ", ",
                                               collapse = ""),
                                        paste0("abs.abs_",
                                               c("records",
                                                 "annual_possible",
                                                 "annual_absent_total",
                                                 "summed_possible",
                                                 "summed_absent_total",
                                                 "summed_absent_illness_medical",
                                                 "summer6th_possible",
                                                 "summer6th_absent_total",
                                                 "summer6th_absent_illness_medical"),
                                               collapse = ", "),
                                        "FROM birth_cohort_sen AS sen",
                                        "FULL OUTER JOIN birth_cohort_absence AS abs",
                                        "ON (sen.dfe_id = abs.dfe_id AND",
                                        "sen.academic_year_start = abs.academic_year_start);"))

  setDT(sen_absence_data)

  dup_recs <- sen_absence_data[, .N, by = .(dfe_id, senabs_academic_year_start)][N > 1]

  sen_absence_data <- merge(sen_absence_data,
                            dup_recs,
                            by = c("dfe_id", "senabs_academic_year_start"),
                            all = TRUE)

  sen_absence_data_dup <- copy(sen_absence_data[!is.na(N)])
  sen_absence_data <- sen_absence_data[is.na(N)]
  sen_absence_data[, N := NULL]

  common_cols <- c("dfe_id",
                   "senabs_academic_year_start",
                   "senabs_school_start_year")

  sen_cols <- c(common_cols,
                paste0("sen_",
                       c("NC_year_actual",
                         "status",
                         "primary_type",
                         "secondary_type",
                         "any_special_provision")))

  abs_cols <- c(common_cols,
                paste0("abs_",
                       c("records",
                         "annual_possible",
                         "annual_absent_total",
                         "summed_possible",
                         "summed_absent_total",
                         "summed_absent_illness_medical",
                         "summer6th_possible",
                         "summer6th_absent_total",
                         "summer6th_absent_illness_medical")))

  sen_absence_data_dedup <- merge(sen_absence_data_dup[is.na(abs_records), .SD, .SDcols = sen_cols],
                                  sen_absence_data_dup[is.na(sen_NC_year_actual), .SD, .SDcols = abs_cols],
                                  by = common_cols,
                                  all = TRUE)

  sen_absence_data <- rbind(sen_absence_data,
                            sen_absence_data_dedup)

  stopifnot(sen_absence_data[, .N, by = .(dfe_id, senabs_academic_year_start)][N > 1, .N] == 0)

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_cohort_sen;",
                                        "DROP TABLE IF EXISTS birth_cohort_absence;"),
                      query_type = "SEND")
  gc()

  sen_absence_data[, senabs_school_year_nominal := senabs_academic_year_start - senabs_school_start_year]

  sen_absence_data <- sen_absence_data[senabs_school_year_nominal > -3 & senabs_school_year_nominal < 14]

  sen_absence_data[, senabs_school_year_nominal := paste0("Y",
                                                          sprintf("%02d",
                                                                  senabs_school_year_nominal))]

  sen_absence_data[, senabs_school_start_year := NULL]

  val_vars <- colnames(sen_absence_data)
  val_vars <- val_vars[!(val_vars %in% c("dfe_id",
                                         "senabs_school_year_nominal"))]

  sen_absence_data_wide <- dcast(sen_absence_data,
                                 dfe_id ~ senabs_school_year_nominal,
                                 value.var = val_vars)


  setDF(sen_absence_data_wide)

  write_data <- writeParquetFile(sen_absence_data_wide,
                                 dir_path = education_data_path,
                                 filename = absence_send_combined_name)


  rm(sen_absence_data,
     sen_absence_data_wide)
  gc()

  return(write_data)
}


