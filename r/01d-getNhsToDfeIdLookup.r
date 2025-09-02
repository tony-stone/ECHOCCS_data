getNhsToDfeIdLookup <- function(ms_conn,
                                  data_directory_path,
                                  table_name) {

  # ms_conn <- mssql_conn
  # data_directory_path <- echoccs_data_directory_path
  # table_name <- nhs_dfe_id_lookup_name

  sql_query <- paste("SELECT ECHILD_PUPIL_REF_MATCHING_ANONYMOUS AS dfe_id,",
                     "pseudo AS nhs_id",
                     "FROM nic381972_echild_bridging;")

  dfe_nhs_lookup_data <- executeQuery(ms_conn,
                                      sql_query)

  setDT(dfe_nhs_lookup_data)

  setorder(dfe_nhs_lookup_data, dfe_id)
  dfe_nhs_lookup_data[, ':=' (dfe_id_row = 1:.N,
                              dfe_id_problematic = as.integer(.N >  1)),
                              by = nhs_id]

  dfe_nhs_lookup_data <- dfe_nhs_lookup_data[dfe_id_row == 1]
  dfe_nhs_lookup_data[, dfe_id_row := NULL]

  write_result <- writeParquetFile(dfe_nhs_lookup_data,
                                   dir_path = paste0(data_directory_path,
                                                     table_name),
                                   filename = table_name)

  rm(dfe_nhs_lookup_data)
  gc()

  return(write_result)
}


