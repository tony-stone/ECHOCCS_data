saveMothersInfo <- function(ms_conn,
                           ddb_conn,
                           hes_apc_tables,
                           echoccs_data_directory_path,
                           mi_name) {

  # ddb_conn <- duckdb_conn
  # ms_conn <- mssql_conn
  # mi_name <- mothers_information_name

  # Save all mother's records

  fields_of_interest <- c("epikey",
                          "token_person_id",
                          "admidate",
                          "classpat",
                          "disdate",
                          "epistart",
                          "epiend",
                          "epiorder",
                          "epitype",
                          "msoa01",
                          "msoa11",
                          "imd04_decile",
                          "resgor",
                          "cr_residence")

  sql_query_deliveries <- paste("SELECT",
                                "mtokenid,",
                                "mdel_my",
                                "FROM MB_id_list",
                                "GROUP BY mtokenid, mdel_my;")

  mothers_deliveries <- executeQuery(ms_conn,
                                     sql_query_deliveries)

  mothers_deliveries_filepath <- writeParquetFile(mothers_deliveries,
                                                  dir_path = paste0(echoccs_data_directory_path,
                                                                    mi_name),
                                                  filename = "mothers_deliveries")

  rm(mothers_deliveries)
  gc()

  sql_query_episodes <- paste("SELECT",
                              "dat.*",
                              "FROM (",
                              "SELECT mtokenid FROM MB_id_list",
                              "GROUP BY mtokenid",
                              ") AS mb",
                              "INNER JOIN (",
                              paste("SELECT",
                                    substring(hes_apc_tables, 21),
                                    "AS reporting_year_start,",
                                    paste0(fields_of_interest,
                                           collapse = ", "),
                                    "FROM",
                                    hes_apc_tables,
                                    "WHERE",
                                    "epistat = '3'",
                                    collapse = " UNION ALL "),
                              ") AS dat ON",
                              "mb.mtokenid = dat.token_person_id;")

  mothers_records <- executeQuery(ms_conn,
                                  sql_query_episodes)

  mothers_records_filepath <- writeParquetFile(mothers_records,
                                               dir_path = paste0(echoccs_data_directory_path,
                                                                 mi_name),
                                               filename = "mothers_episodes")

  rm(mothers_records)
  gc()


  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_deliveries;",
                                        "CREATE TABLE mothers_deliveries AS (",
                                        "SELECT mtokenid AS token_person_id,",
                                        "TRY_CAST(CONCAT(SUBSTRING(mdel_my, 1, 4), CASE WHEN LENGTH(mdel_my) = 6 THEN '-0' ELSE '-' END, SUBSTRING(mdel_my, 6, 2), '-01') AS DATE) AS mb_mdel_my",
                                        "FROM",
                                        paste0("read_parquet('",
                                               mothers_deliveries_filepath,
                                               "')"),
                                        ");"),
                      query_type = "SEND")


  date_fields <- c("admidate",
                   "disdate",
                   "epiend",
                   "epistart")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_records;",
                                        "CREATE TABLE mothers_records AS (",
                                        "SELECT",
                                        "epikey,",
                                        "token_person_id,",
                                        "CAST(CASE WHEN classpat = '5' THEN 0 WHEN classpat = '8' THEN 1 ELSE 2 END AS TINYINT) AS classpat_order,",
                                        "CAST(CASE WHEN epitype = '2' THEN 0 WHEN epitype = '5' THEN 1 WHEN epitype = '3' THEN 2 WHEN epitype = '6' THEN 3 ELSE 4 END AS TINYINT) AS epitype_order,",
                                        "CAST(CASE WHEN epiorder IN ('', '98', '99') THEN NULL ELSE epiorder END AS TINYINT) AS epiorder,",
                                        paste0("TRY_CAST(CASE WHEN ",
                                               date_fields,
                                               " IN ('1800-01-01', '1801-01-01') ",
                                               "THEN NULL ELSE ",
                                               date_fields,
                                               " END AS DATE) AS ",
                                               date_fields,
                                               collapse = ", "),
                                        ",",
                                        "msoa01,",
                                        "msoa11,",
                                        "imd04_decile,",
                                        "resgor,",
                                        "cr_residence",
                                        "FROM",
                                        paste0("read_parquet('",
                                               mothers_records_filepath,
                                               "')"),
                                        ");"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_records_1;",
                                        "CREATE TABLE mothers_records_1 AS (",
                                        "SELECT *,",
                                        "LEAST(",
                                        paste0(date_fields,
                                               collapse = ", "),
                                        ") AS episode_min_date,",
                                        "GREATEST(",
                                        paste0(date_fields,
                                               collapse = ", "),
                                        ") AS episode_max_date",
                                        "FROM mothers_records);",
                                        "DROP TABLE IF EXISTS mothers_records;"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_records_2;",
                                        "CREATE TABLE mothers_records_2 AS (",
                                        "SELECT *,",
                                        "DATE_TRUNC('MONTH', episode_min_date) AS episode_min_month,",
                                        "DATE_TRUNC('MONTH', episode_max_date) AS episode_max_month",
                                        "FROM mothers_records_1);",
                                        "DROP TABLE IF EXISTS mothers_records_1;"),
                      query_type = "SEND")


  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_deliveries_records;",
                                        "CREATE TABLE mothers_deliveries_records AS (",
                                        "SELECT",
                                        "ROW_NUMBER() OVER (PARTITION BY md.token_person_id, md.mb_mdel_my ORDER BY ABS(DATEDIFF('MONTH', md.mb_mdel_my, mr.episode_min_date)), mr.epitype_order, mr.classpat_order, mr.episode_min_date) AS delivery_epi_order,",
                                        "md.mb_mdel_my,",
                                        "mr.*",
                                        "FROM mothers_deliveries AS md",
                                        "INNER JOIN mothers_records_2 AS mr ON",
                                        "md.token_person_id = mr.token_person_id AND",
                                        "mr.episode_min_month <= md.mb_mdel_my AND mr.episode_min_month >= md.mb_mdel_my);"),
                      query_type = "SEND")

  mothers_information_data <- executeQuery(ddb_conn,
                                           sql_query = paste("SELECT",
                                                             "token_person_id AS mi_token_person_id_mother,",
                                                             "mb_mdel_my AS mi_mdel_my,",
                                                             "msoa01 AS mi_msoa01,",
                                                             "msoa11 AS mi_msoa11,",
                                                             "imd04_decile AS mi_imd04_decile,",
                                                             "resgor AS mi_resgor,",
                                                             "cr_residence AS mi_cr_residence,",
                                                             "episode_min_date AS mi_episode_min_date ,",
                                                             "episode_max_date AS mi_episode_max_date",
                                                             "FROM mothers_deliveries_records WHERE delivery_epi_order = 1;"))

  mothers_information_filepath <- writeParquetFile(mothers_information_data,
                                                   dir_path = paste0(echoccs_data_directory_path,
                                                                     mi_name),
                                                   filename = mi_name)

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS mothers_deliveries_records;",
                                        "DROP TABLE IF EXISTS mothers_deliveries;",
                                        "DROP TABLE IF EXISTS mothers_records_2;"),
                      query_type = "SEND")

  rm(mothers_information_data)
  gc()

  unlink(mothers_deliveries_filepath)
  unlink(mothers_records_filepath)

  return(mothers_information_filepath)
}
