collateEduCensusData <- function(conn,
                                 echoccs_data_directory_path,
                                 education_data_directory,
                                 edu_census_name) {

  # conn <- mssql_conn

  census_data_mapping <- fread("echild_edu_field_name_mappings/school_census_pupil_level_field_name_mappings_spring.csv")

  census2011_2022_indices <- which(census_data_mapping$table %in% c("Autumn_Census_20",
                                                                    "Spring_Census_20",
                                                                    "Summer_Census_20"))

  census_data_mapping <- rbind(census_data_mapping[-census2011_2022_indices],
                               census_data_mapping[rep(census2011_2022_indices, length(11:22))][, table := paste0(table, rep(11:22,
                                                                                                                             each = length(census2011_2022_indices)))])
  msoa0111_indices <- which(census_data_mapping$col_source == "MSOA" & substr(census_data_mapping$table,
                                                                              nchar(census_data_mapping$table) - 1,
                                                                              nchar(census_data_mapping$table)) %in% as.character(13:14))

  census_data_mapping[col_source == "MSOA01_SPR" & substr(table,
                                                          nchar(table) - 1,
                                                          nchar(table)) %in% as.character(11:12),
                      col_source := "MSOA_SPR"]

  census_data_mapping[col_source == "MSOA11_SPR" & substr(table,
                                                    nchar(table) - 1,
                                                    nchar(table)) %in% as.character(11:12),
                      col_source := "NULL"]

  census_data_mapping[col_source == "MSOA01_SPR" & substr(table,
                                                    nchar(table) - 1,
                                                    nchar(table)) %in% as.character(15:22),
                      col_source := "NULL"]

  census_data_mapping[substr(table, nchar(table) - 1, nchar(table)) %in% as.character(11:22) &
                        !(substr(col_target, 1, 11) %in% c("census_imd_", "census_term")) &
                        col_source != "NULL",
                      col_source := paste0(col_source,
                                           substr(table, nchar(table) - 1, nchar(table)))]

  census_data_mapping[col_source != "NULL" &
                        col_target %in% c("census_ethnic_group",
                                          "census_FSM_eligible",
                                          "census_part_time",
                                          "census_special_provision_indicator"),
                      col_source := paste0("CAST(" ,
                                           col_source,
                                           " AS VARCHAR(50))")]

  sql_census_data_vector <- sapply(unique(census_data_mapping$table), function(tbl, lookup) {
    return(paste("SELECT",
                 paste0(lookup[table == tbl, col_source],
                        " AS ",
                        lookup[table == tbl, col_target],
                        collapse = ", "),
                 "FROM",
                 tbl))
  }, lookup = census_data_mapping)

  sql_census_data <- paste(sql_census_data_vector,
                           collapse = " UNION ALL ")

  collated_census_data <- executeQuery(conn, sql_census_data)

  write_result <- writeParquetFile(collated_census_data,
                                   dir = paste0(echoccs_data_directory_path,
                                                education_data_directory),
                                   filename = edu_census_name)

  return(write_result)
}


