saveBirthInfo <- function(ddb_conn,
                          ms_conn,
                          echoccs_data_directory_path,
                          infants_records_name,
                          birth_information_name) {

  # ddb_conn <- duckdb_conn
  # ms_conn <- mssql_conn

  # Get mother-baby link data

  mb_fields <- c("tokenid AS token_person_id",
                 "mtokenid AS mb_token_person_id_mother",
                 "link AS mb_link",
                 "matage_clean AS mb_matage",
                 "gestat_clean AS mb_gestat",
                 "birweit_clean AS mb_birweit",
                 "sexbaby_clean AS mb_sexbaby",
                 "delmeth_clean AS mb_delmeth",
                 "birstat_clean AS mb_birstat",
                 "sibling_n AS mb_sibling_n",
                 "imd04_decile_cat_clean AS mb_imd04_decile",
                 "mdel_my AS mb_mdel_my")

  mb_sql_query <- paste("SELECT",
                        paste0(mb_fields,
                               collapse = ", "),
                        "FROM MB_id_list;")

  mb_links_data <- executeQuery(ms_conn,
                                mb_sql_query)

  mb_links_filepath <- writeParquetFile(mb_links_data,
                                        dir_path = paste0(echoccs_data_directory_path,
                                                          birth_information_name),
                                        filename = "mother_baby_links")

  rm(mb_links_data)
  gc()

  # Infant records

  filepath <- paste0(echoccs_data_directory_path,
                     infants_records_name,
                     "/",
                     infants_records_name,
                     ".parquet")

  date_fields <- c("admidate",
                   "disdate",
                   "epiend",
                   "epistart")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details_raw;",
                                        "CREATE TABLE birth_details_raw AS (",
                                        "SELECT token_person_id,",
                                        paste0("TRY_CAST(CASE WHEN ",
                                               date_fields,
                                               " IN ('1800-01-01', '1801-01-01') ",
                                               "THEN NULL ELSE ",
                                               date_fields,
                                               " END AS DATE) AS ",
                                               date_fields,
                                               collapse = ", "),
                                        ", ",
                                        paste0("TRY_CAST(CASE WHEN mydob IN ",
                                               "('011800', '011801') THEN NULL ",
                                               "ELSE CONCAT(",
                                               "SUBSTRING(mydob, 3, 4), '-', ",
                                               "SUBSTRING(mydob, 1, 2), '-01') END AS DATE) ",
                                               "AS mydob,"),
                                        "TRY_CAST(CASE WHEN birweit = '9999' THEN NULL ELSE birweit END AS INT) AS birweit,",
                                        "CASE WHEN TRY_CAST(CASE WHEN matage = '999' THEN NULL ELSE matage END AS INT) < 8 OR TRY_CAST(CASE WHEN matage = '999' THEN NULL ELSE matage END AS INT) > 60 THEN NULL ELSE TRY_CAST(CASE WHEN matage = '999' THEN NULL ELSE matage END AS INT) END AS matage,",
                                        "TRY_CAST(CASE WHEN gestat = '99' THEN NULL ELSE gestat END AS INT) AS gestat,",
                                        "sexbaby,",
                                        "ethnos,",
                                        "msoa01,",
                                        "msoa11,",
                                        "imd04_decile",
                                        "FROM",
                                        paste0("read_parquet('",
                                               filepath,
                                               "')"),
                                        ");"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details_temp;",
                                        "CREATE TABLE birth_details_temp AS (",
                                        "SELECT *,",
                                        "CASE WHEN birweit IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN matage IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN gestat IS NOT NULL THEN 1 ELSE 0 END AS num_non_null,",
                                        "LEAST(",
                                        paste0(date_fields,
                                               collapse = ", "),
                                        ") AS episode_min_date",
                                        "FROM birth_details_raw);",
                                        "DROP TABLE IF EXISTS birth_details_raw;"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details;",
                                        "CREATE TABLE birth_details AS (",
                                        "SELECT dets.*,",
                                        "ROW_NUMBER() OVER (PARTITION BY dets.token_person_id ORDER BY dets.episode_min_date, dets.admidate, dets.epistart, dets.epiend) AS tpi_record,",
                                        "DATE_DIFF('MONTH', mydob, episode_min_date) AS months_since_dob,",
                                        "agg.any_null_epi_date,",
                                        "agg.n_mydob,",
                                        "agg.bi_records_within_1month,",
                                        "agg.bi_records_before_8yrs",
                                        "FROM birth_details_temp AS dets",
                                        "INNER JOIN (",
                                        "SELECT token_person_id,",
                                        "MAX(CASE WHEN episode_min_date IS NULL THEN 1 ELSE 0 END) AS any_null_epi_date,",
                                        "COUNT(DISTINCT mydob) AS n_mydob,",
                                        "SUM(CASE WHEN ABS(DATE_DIFF('MONTH', mydob, episode_min_date)) < 2 AND num_non_null > 0 THEN 1 ELSE 0 END) AS bi_records_within_1month,",
                                        "SUM(CASE WHEN DATE_DIFF('MONTH', mydob, episode_min_date) <= 95 AND num_non_null > 0 THEN 1 ELSE 0 END) AS bi_records_before_8yrs",
                                        "FROM birth_details_temp",
                                        "GROUP BY token_person_id) AS agg",
                                        "ON dets.token_person_id = agg.token_person_id);",
                                        "DROP TABLE IF EXISTS birth_details_temp;"),
                      query_type = "SEND")


  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details_2;",
                                        "CREATE TABLE birth_details_2 AS (",
                                        "SELECT bd.*,",
                                        "bi.bi_order",
                                        "FROM birth_details AS bd",
                                        "LEFT JOIN (",
                                        "SELECT token_person_id, tpi_record,",
                                        "ROW_NUMBER() OVER (PARTITION BY token_person_id ORDER BY tpi_record) AS bi_order",
                                        "FROM birth_details WHERE",
                                        "num_non_null > 0",
                                        "AND ABS(months_since_dob) < 2) AS bi",
                                        "ON bd.token_person_id = bi.token_person_id AND",
                                        "bd.tpi_record = bi.tpi_record);",
                                        "DROP TABLE IF EXISTS birth_details;"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details_3;",
                                        "CREATE TABLE birth_details_3 AS (",
                                        "SELECT bd.*,",
                                        "nn.max_num_non_null",
                                        "FROM birth_details_2 AS bd",
                                        "LEFT JOIN (",
                                        "SELECT token_person_id, MAX(num_non_null) AS max_num_non_null",
                                        "FROM birth_details_2 WHERE",
                                        "bi_order IS NOT NULL",
                                        "GROUP BY token_person_id) AS nn",
                                        "ON bd.token_person_id = nn.token_person_id);",
                                        "DROP TABLE IF EXISTS birth_details_2;"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = paste("DROP TABLE IF EXISTS birth_details_summary;",
                                        "CREATE TABLE birth_details_summary AS (",
                                        "SELECT",
                                        "CASE WHEN bi.token_person_id IS NULL THEN mb.token_person_id ELSE bi.token_person_id END AS token_person_id,",
                                        "CASE WHEN bi.bi_order = 1 THEN 1 ELSE 0 END AS bi_valid,",
                                        "bi.admidate AS bi_admidate,",
                                        "bi.episode_min_date AS bi_episode_min_date,",
                                        "bi.bi_records_before_8yrs,",
                                        "bi.gestat AS bi_gestat,",
                                        "bi.birweit AS bi_birweit,",
                                        "bi.matage AS bi_matage,",
                                        "bi.sexbaby AS bi_sexbaby,",
                                        "bi.imd04_decile AS bi_imd04_decile,",
                                        "mb.mb_token_person_id_mother,",
                                        "TRY_CAST(mb.mb_matage AS INT) AS mb_matage,",
                                        "TRY_CAST(mb.mb_gestat AS INT) AS mb_gestat,",
                                        "TRY_CAST(mb.mb_birweit AS INT) AS mb_birweit,",
                                        "mb.mb_sexbaby,",
                                        "mb.mb_imd04_decile,",
                                        "TRY_CAST(CONCAT(SUBSTRING(mb.mb_mdel_my, 1, 4), CASE WHEN LENGTH(mb.mb_mdel_my) = 6 THEN '-0' ELSE '-' END, SUBSTRING(mb.mb_mdel_my, 6, 2), '-01') AS DATE) AS mb_mdel_my",
                                        "FROM (",
                                        "SELECT * FROM birth_details_3 WHERE bi_order = 1",
                                        "UNION ALL",
                                        "SELECT * FROM birth_details_3 WHERE tpi_record = 1 AND max_num_non_null IS NULL",
                                        ") AS bi",
                                        "FULL JOIN ",
                                        paste0("read_parquet('",
                                               mb_links_filepath,
                                               "')"),
                                        "AS mb",
                                        "ON bi.token_person_id = mb.token_person_id",
                                        ");"),
                      query_type = "SEND")

  res <- executeQuery(ddb_conn,
                      sql_query = "DROP TABLE IF EXISTS birth_details_3;",
                      query_type = "SEND")

  birth_information_data <- executeQuery(ddb_conn,
                                         sql_query = "SELECT * FROM birth_details_summary;")

  birth_information_filepath <- writeParquetFile(birth_information_data,
                                                 dir_path = paste0(echoccs_data_directory_path,
                                                                   birth_information_name),
                                                 filename = birth_information_name)

  res <- executeQuery(ddb_conn,
                      sql_query = "DROP TABLE IF EXISTS birth_details_summary;",
                      query_type = "SEND")

  unlink(mb_links_filepath)

  return(birth_information_filepath)
}
