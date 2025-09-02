collateEduAbsenceData <- function(conn,
                                  echoccs_data_directory_path,
                                  education_data_directory,
                                  edu_absence_name,
                                  edu_absence_consolidated_name) {

  # conn <- mssql_conn

  absence_data_mapping <- fread("echild_edu_field_name_mappings/school_absence_mappings.csv")

  absence_tables_2005 <- "Absence_2006_3Term"
  absence_tables_2006_2011 <- paste0("Absence_20",
                                     sprintf("%02d", 7:12),
                                     "_3Term")
  absence_tables_2012_2021 <- paste0("Absence_20",
                                     c(13:19, 22),
                                     "_3Term")


  absence_data_mapping_consolidated <- rbind(absence_data_mapping[, .(table = absence_tables_2005,
                                                                      col_source = abs_2005,
                                                                      col_target)],
                                             absence_data_mapping[rep(1:nrow(absence_data_mapping),
                                                                      length(absence_tables_2006_2011)),
                                                                  .(table = rep(absence_tables_2006_2011,
                                                                                each = nrow(absence_data_mapping)),
                                                                    col_source = abs_2006_2011,
                                                                    col_target)],
                                             absence_data_mapping[rep(1:nrow(absence_data_mapping),
                                                                      length(absence_tables_2012_2021)),
                                                                  .(table = rep(absence_tables_2012_2021,
                                                                                each = nrow(absence_data_mapping)),
                                                                    col_source = abs_2012_2021,
                                                                    col_target)])

  absence_data_mapping_consolidated[col_source != "NULL", col_source := paste0(col_source,
                                                       "_ab",
                                                       substr(table, 11, 12))]


  sql_absence_data_vector <- sapply(unique(absence_data_mapping_consolidated$table), function(tbl, lookup) {
    return(paste("SELECT",
                 paste0(lookup[table == tbl, col_source],
                        " AS ",
                        lookup[table == tbl, col_target],
                        collapse = ", "),
                 "FROM",
                 tbl))
  }, lookup = absence_data_mapping_consolidated)

  sql_absence_data <- paste(sql_absence_data_vector,
                           collapse = " UNION ALL ")

  collated_absence_data <- executeQuery(conn, sql_absence_data)

  write_result <- writeParquetFile(collated_absence_data,
                                   dir = paste0(echoccs_data_directory_path,
                                                education_data_directory),
                                   filename = edu_absence_name)

  setDT(collated_absence_data)

  collated_absence_data[abs_academic_year_start := as.integer(substr(abs_academic_year, 1, 4))]

  collated_absence_data_summary <- collated_absence_data[, .(abs_records = .N,
                                                             abs_annual_possible = sum(abs_annual_possible,
                                                                                       na.rm = TRUE),
                                                             abs_annual_absent_total = sum(abs_annual_absent_total,
                                                                                           na.rm = TRUE),
                                                             abs_summed_possible = sum(abs_autumn_possible,
                                                                                       abs_spring_possible,
                                                                                       abs_summer_possible,
                                                                                       na.rm = TRUE),
                                                             abs_summed_absent_total = sum(abs_autumn_absent_total,
                                                                                           abs_spring_absent_total,
                                                                                           abs_summer_absent_total,
                                                                                           na.rm = TRUE),
                                                             abs_summed_absent_illness_medical = sum(abs_autumn_absent_illness,
                                                                                                     abs_spring_absent_illness,
                                                                                                     abs_summer_absent_illness,
                                                                                                     abs_autumn_absent_medical,
                                                                                                     abs_spring_absent_medical,
                                                                                                     abs_summer_absent_medical,
                                                                                                     na.rm = TRUE),
                                                             abs_summer6th_possible = sum(abs_summer6th_possible,
                                                                                          na.rm = TRUE),
                                                             abs_summer6th_absent_total = sum(abs_summer6th_absent_total,
                                                                                              na.rm = TRUE),
                                                             abs_summer6th_absent_illness_medical = sum(abs_summer6th_absent_illness,
                                                                                                        abs_summer6th_absent_medical,
                                                                                                        na.rm = TRUE)),
                                                         by = .(dfe_id, abs_academic_year_start)]

  collated_absence_data_summary[abs_academic_year_start == 2005, ':=' (abs_annual_possible = NA,
                                                                       abs_annual_absent_total = NA,
                                                                       abs_summed_absent_illness_medical = NA)]
  collated_absence_data_summary[abs_academic_year_start < 2012, ':=' (abs_summer6th_possible = NA,
                                                                      abs_summer6th_absent_total = NA,
                                                                      abs_summer6th_absent_illness_medical = NA)]

  setDF(collated_absence_data_summary)

  write_result_summary <- writeParquetFile(collated_absence_data_summary,
                                   dir = paste0(echoccs_data_directory_path,
                                                education_data_directory),
                                   filename = edu_absence_consolidated_name)


  return(write_result_summary)
}


