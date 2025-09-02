collateCongenitalDiagnoses <- function(conn,
                                       echoccs_data_directory_path,
                                       diagnoses_filepath,
                                       congenital_diagnoses_name) {

  # conn <- duckdb_conn

  # Included codes as per EUROCAT Guide 1.5
  # All 5 character codes have been omitted from the inclusion and exclusion
  #   code lists.

  ca_inclusions <- rbind(data.table(diagnosis_3char = paste0("Q",
                                                            sprintf("%02d", c(0:99))),
                                   diagnosis_4char = as.character(NA)),
                        data.table(diagnosis_3char = as.character(NA),
                                   diagnosis_4char = c(paste0("D",
                                                              c(215, 821)),
                                                       paste0("P",
                                                              c(350:351,
                                                                354,
                                                                358,
                                                                371)))))

  ca_exclusions <- rbind(data.table(diagnosis_3char = paste0("Q",
                                                             c(53,
                                                               95)),
                                    diagnosis_4char = as.character(NA)),
                         data.table(diagnosis_3char = as.character(NA),
                                    diagnosis_4char = paste0("Q",
                                                       c(101:103,
                                                         105,
                                                         135,
                                                         170:175,
                                                         179,
                                                         180:182,
                                                         184:187,
                                                         189,
                                                         246,
                                                         250,
                                                         256,
                                                         261,
                                                         270,
                                                         314:315,
                                                         320,
                                                         322,
                                                         331,
                                                         357,
                                                         381:382,
                                                         400:401,
                                                         430,
                                                         444,
                                                         501:502,
                                                         505,
                                                         523,
                                                         525,
                                                         527,
                                                         544,
                                                         610,
                                                         627,
                                                         633,
                                                         653:656,
                                                         658:659,
                                                         661:669,
                                                         670:678,
                                                         680,
                                                         683:685,
                                                         752:753,
                                                         760,
                                                         765,
                                                         825,
                                                         833,
                                                         845:846,
                                                         899,
                                                         850:851))))

  ca_exclusions[diagnosis_4char %in% c("Q250", "Q256"), ca_group := "ga_mte_37wks_only"]
  ca_exclusions[diagnosis_4char %in% "Q850", ca_group := "neurofibromatosis"]
  ca_exclusions[diagnosis_4char %in% "Q851", ca_group := "tuberous_sclerosis"]
  ca_exclusions[, diagnosis_4char_3 := substr(diagnosis_4char, 1, 3)]

  ca_inclusions <- ca_inclusions[!(diagnosis_3char %in% ca_exclusions[!is.na(diagnosis_3char), diagnosis_3char])]
  ca_inclusions <- ca_inclusions[rep(1:nrow(ca_inclusions), times = fifelse(diagnosis_3char %in% ca_exclusions$diagnosis_4char_3, 10, 1))]
  ca_inclusions[!is.na(diagnosis_3char) &
                  diagnosis_3char %in% ca_exclusions$diagnosis_4char_3, ':=' (diagnosis_4char = paste0(diagnosis_3char,
                                                                                                       rep(0:9, sum(!is.na(diagnosis_3char) & diagnosis_3char %in% ca_exclusions$diagnosis_4char_3) / 10)),
                                                                              diagnosis_3char = NA)]

  ca_inclusions <- ca_inclusions[!(diagnosis_4char %in% ca_exclusions[!is.na(diagnosis_4char) & is.na(ca_group), diagnosis_4char])]
  ca_inclusions <- merge(ca_inclusions,
                         ca_exclusions[!is.na(ca_group), .(diagnosis_4char, ca_group)],
                         by = "diagnosis_4char",
                         all.x = TRUE)

  ca_inclusions[diagnosis_3char == "Q90", ca_group := "down_syndrome"]

  ca_inclusions[is.na(ca_group), ca_group := "other"]

  # fwrite(ca_inclusions,
  #        "data-out/2025-08-07_congenital_diagnoses_inclusion.csv")

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE IF EXISTS congenital_diagnoses_inc;",
                      query_type = "SEND")

  dbWriteTable(conn,
               "congenital_diagnoses_inc",
               ca_inclusions)

  date_fields <- c("admidate",
                   "epistart",
                   "epiend",
                   "disdate")

  sql_diagnoses_temp <-paste("DROP TABLE IF EXISTS congenital_diagnoses_temp;",
                             "CREATE TABLE congenital_diagnoses_temp AS (",
                             "SELECT",
                             "cd.ca_group,",
                             "diags.token_person_id,",
                             paste0("CAST(CASE WHEN diags.",
                                    date_fields,
                                    " IN ('', '1800-01-01', '1801-01-01') THEN NULL ",
                                    "ELSE diags.", date_fields, " END ",
                                    "AS DATE) AS ",
                                    date_fields,
                                    collapse = ", "),
                             "FROM congenital_diagnoses_inc AS cd",
                             "INNER JOIN",
                             paste0("read_parquet('",
                                    diagnoses_filepath,
                                    "/*/*.parquet', ",
                                    "hive_partitioning = true)"),
                             "AS diags ON (",
                             "cd.diagnosis_3char = diags.diagnosis_3char OR",
                             "cd.diagnosis_4char = diags.diagnosis",
                             "));",
                             "DROP TABLE IF EXISTS congenital_diagnoses_inc;")

  res <- executeQuery(conn,
                      sql_query = sql_diagnoses_temp,
                      query_type = "SEND")

  sql_diagnoses <- paste("DROP TABLE IF EXISTS congenital_diagnoses;",
                         "CREATE TABLE congenital_diagnoses AS (",
                         "SELECT",
                         "token_person_id,",
                         "ca_group,",
                         "LEAST(",
                         paste0(date_fields,
                                collapse = ", "),
                         ") AS min_date",
                         "FROM congenital_diagnoses_temp);",
                         "DROP TABLE IF EXISTS congenital_diagnoses_temp;")

  res <- executeQuery(conn,
                      sql_query = sql_diagnoses,
                      query_type = "SEND")

  missing_all_dates <- executeQuery(conn,
                                    paste("SELECT COUNT(*) AS missing_all_dates",
                                          "FROM congenital_diagnoses",
                                          "WHERE min_date IS NULL"))

  if(missing_all_dates > 0) stop("All dates missing.")

  sql_first_diagnosis <- paste("SELECT",
                               "token_person_id,",
                               "ca_group,",
                               "MIN(min_date) AS min_date",
                               "FROM congenital_diagnoses",
                               "GROUP BY token_person_id, ca_group;")

  first_diagnosis <- executeQuery(conn,
                                  sql_query = sql_first_diagnosis)

  setDT(first_diagnosis)

  first_diagnosis_wide <- dcast(first_diagnosis,
                                token_person_id ~ ca_group,
                                value.var = "min_date")

  first_diagnosis_wide[, other := pmin(other,
                                       down_syndrome,
                                       neurofibromatosis,
                                       tuberous_sclerosis,
                                       na.rm = TRUE)]

  first_diagnosis_wide[other <= ga_mte_37wks_only,
                       ga_mte_37wks_only := NA]

  res <- executeQuery(conn,
                      sql_query = "DROP TABLE congenital_diagnoses;",
                      query_type = "SEND")

  setnames(first_diagnosis_wide,
           colnames(first_diagnosis_wide)[-1],
           paste0("ca_",
                  colnames(first_diagnosis_wide)[-1]))

  setDF(first_diagnosis_wide)
  write_result <- writeParquetFile(first_diagnosis_wide,
                                   dir_path = paste0(echoccs_data_directory_path,
                                                     congenital_diagnoses_name),
                                   filename = congenital_diagnoses_name)

  rm(first_diagnosis,
     first_diagnosis_wide)
  gc()

  return(write_result)
}
