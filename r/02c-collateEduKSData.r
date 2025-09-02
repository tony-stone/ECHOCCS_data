collateEduKSData <- function(ms_conn,
                             ddb_conn,
                             echoccs_data_directory_path,
                             education_data_directory,
                             edu_census_name) {

  # ms_conn <- mssql_conn
  # ddb_conn <- duckdb_conn


# Key Stage 1 census info -------------------------------------------------

  sql_query_ks1_census <- paste("SELECT",
                                "dfe_id,",
                                "census_NC_year_actual,",
                                "CAST(SUBSTRING(census_academic_year, 1, 4) AS SMALLINT) AS census_academic_year_start",
                                "FROM",
                                paste0("read_parquet('",
                                       echoccs_data_directory_path,
                                       edu_census_path,
                                       "/",
                                       edu_census_name,
                                       ".parquet')"),
                                "WHERE census_NC_year_actual = '2'",
                                "AND census_academic_year >= '2003/2004'",
                                "GROUP BY dfe_id, census_NC_year_actual, census_academic_year_start;")

  collated_ks1_census_data <- executeQuery(ddb_conn, sql_query_ks1_census)

  write_ks1_census_data <- writeParquetFile(collated_ks1_census_data,
                                            dir_path = paste0(echoccs_data_directory_path,
                                                              education_data_directory),
                                            filename = "ks1_census_data")


  rm(collated_ks1_census_data)
  gc()



  # Key Stage 1 -------------------------------------------------------------



  sql_query_ks1 <- paste("SELECT ",
                         "KS1_PupilMatchingRefAnonymous AS dfe_id,",
                         "CAST(SUBSTRING(KS1_ACADYR, 1, 4) AS SMALLINT) AS ks1_academic_year_start,",
                         "ks1_laestab_anon AS ks1_school_id,",
                         "KS1_ToE_CODE AS ks1_school_type_edubase,",
                         "KS1_NFTYPE AS ks1_school_type_aat,",
                         "KS1_GENDER AS ks1_gender,",
                         "KS1_YEAROFBIRTH AS ks1_birth_year,",
                         "KS1_MONTHOFBIRTH AS ks1_birth_month,",
                         "KS1_TRIALFLAG AS ks1_2003_school_trial_status,",
                         "KS1_READING AS ks1_reading_1997_2014,",
                         "KS1_READ_OUTCOME AS ks1_reading_2015_2021,",
                         "KS1_WRITING AS ks1_writing_1997_2014,",
                         "KS1_WRIT_OUTCOME AS ks1_writing_2015_2021,",
                         "KS1_MATHS AS ks1_maths_1997_2014,",
                         "KS1_MATH_OUTCOME AS ks1_maths_2015_2021",
                         "FROM KS1_1998_to_2019_and_2022",
                         "WHERE KS1_ACADYR >= '2003/2004';")

  collated_ks1_data <- executeQuery(ms_conn, sql_query_ks1)

  # In 2003/04 and 2004/05, a small number of pupils had more than one record within a given academic year.

  setDT(collated_ks1_data)
  collated_ks1_data_1997_2004 <- copy(collated_ks1_data[ks1_academic_year_start <= 2004])
  collated_ks1_data <- collated_ks1_data[ks1_academic_year_start > 2004]

  # unique(c(collated_ks1_data_1997_2004$ks1_reading_1997_2014,
  #   collated_ks1_data_1997_2004$ks1_writing_1997_2014,
  #   collated_ks1_data_1997_2004$ks1_maths_1997_2014))

  ks1_grades <- c("D", "W", "1", "2", "2C", "2B", "2A", "3", "4", "4+")
  ks1_level2plus_grades <- ks1_grades[-(1:3)]

  collated_ks1_data_1997_2004[, ':=' (ks1_level2plus_pases = as.integer(ks1_reading_1997_2014 %in% ks1_level2plus_grades) +
                                        as.integer(ks1_writing_1997_2014 %in% ks1_level2plus_grades) +
                                        as.integer(ks1_maths_1997_2014 %in% ks1_level2plus_grades),
                                      ks1_reading_points = match(ks1_reading_1997_2014, ks1_grades),
                                      ks1_writing_points = match(ks1_writing_1997_2014, ks1_grades),
                                      ks1_maths_points = match(ks1_maths_1997_2014, ks1_grades))]



  collated_ks1_data_1997_2004[, ks1_points := rowSums(.SD, na.rm = TRUE), .SDcols = c("ks1_reading_points",
                                                                                      "ks1_writing_points",
                                                                                      "ks1_maths_points")]

  setorder(collated_ks1_data_1997_2004, -ks1_level2plus_pases, -ks1_points)
  collated_ks1_data_1997_2004[, record_order := 1:.N, by = .(dfe_id, ks1_academic_year_start)]

  collated_ks1_data_1997_2004 <- collated_ks1_data_1997_2004[record_order == 1]

  collated_ks1_data_1997_2004[, c("ks1_level2plus_pases",
                                  "ks1_reading_points",
                                  "ks1_writing_points",
                                  "ks1_maths_points",
                                  "ks1_points",
                                  "record_order") := NULL]

  collated_ks1_data <- rbind(collated_ks1_data,
                             collated_ks1_data_1997_2004)

  write_ks1_data <- writeParquetFile(collated_ks1_data,
                                     dir_path = paste0(echoccs_data_directory_path,
                                                       education_data_directory),
                                     filename = "ks1_data")


  rm(collated_ks1_data,
     collated_ks1_data_1997_2004)
  gc()



  # Key Stage 2 -------------------------------------------------------------



  sql_query_ks2 <- paste("SELECT",
                         "KS2_PupilMatchingRefAnonymous AS dfe_id,",
                         "CAST(SUBSTRING(KS2_ACADYR, 1, 4) AS SMALLINT) AS ks2_academic_year_start,",
                         "ks2_laestab_anon AS ks2_school_id,",
                         "KS2_ToE_CODE AS ks2_school_type_edubase,",
                         "KS2_NFTYPE AS ks2_school_type_aat,",
                         "KS2_GENDER AS ks2_gender,",
                         "KS2_YEAROFBIRTH AS ks2_birth_year,",
                         "KS2_MONTHOFBIRTH AS ks2_birth_month,",
                         "KS2_NATRES AS ks2_dfe_national_result_included,",
                         "KS2_READMRK AS ks2_reading_mark,",
                         "KS2_READLEV AS ks2_reading_1998_2014,",
                         "KS2_READOUTCOME AS ks2_reading_2015_onward,",
                         "KS2_READHIGH AS ks2_reading_above_expected_level_2015_onward,",
                         "KS2_READEXP AS ks2_reading_at_or_above_expected_level_2015_onward,",
                         "KS2_MATTOTMRK AS ks2_maths_mark_1995_2014,",
                         "KS2_MATMRK AS ks2_maths_mark_2015_onward,",
                         "KS2_MATLEV AS ks2_maths_1995_2014,",
                         "KS2_MATOUTCOME AS ks2_maths_2015_onward,",
                         "KS2_MATHIGH AS ks2_maths_above_expected_level_2015_onward,",
                         "KS2_MATEXP AS ks2_maths_at_or_above_expected_level_2015_onward",
                         "FROM KS2Pupil_1996_to_2019_and_2022",
                         "WHERE KS2_ACADYR >= '2007/2008';")

  collated_ks2_data <- executeQuery(ms_conn, sql_query_ks2)

  setDT(collated_ks2_data)

  write_ks2_data <- writeParquetFile(collated_ks2_data,
                                     dir_path = paste0(echoccs_data_directory_path,
                                                       education_data_directory),
                                     filename = "ks2_data_all")

  # A small number of pupils had more than one record within a given academic year.


  collated_ks2_data_pre2015 <- copy(collated_ks2_data[ks2_academic_year_start <= 2014])
  collated_ks2_data <- copy(collated_ks2_data[ks2_academic_year_start > 2014])

  # KS2 Pre-2015

  # unique(c(collated_ks2_data$ks2_reading_1998_2014,
  #   collated_ks2_data$ks2_maths_1995_2014))

  ks2_pre2015_grades <- c("B", "N", "T", "2", "3", "4", "5", "6")
  ks2_pre2015_level4plus_grades <- ks2_pre2015_grades[-(1:5)]

  collated_ks2_data_pre2015[, ':=' (ks2_level4plus_passes = as.integer(ks2_reading_1998_2014 %in% ks2_pre2015_level4plus_grades) +
                                      as.integer(ks2_maths_1995_2014 %in% ks2_pre2015_level4plus_grades),
                                    ks2_reading_points = match(ks2_reading_1998_2014, ks2_pre2015_grades),
                                    ks2_maths_points = match(ks2_maths_1995_2014, ks2_pre2015_grades))]



  collated_ks2_data_pre2015[, ks2_points := rowSums(.SD, na.rm = TRUE), .SDcols = c("ks2_reading_points",
                                                                                    "ks2_maths_points")]

  setorder(collated_ks2_data_pre2015, -ks2_dfe_national_result_included, -ks2_level4plus_passes, -ks2_points)
  collated_ks2_data_pre2015[, record_order := 1:.N, by = .(dfe_id, ks2_academic_year_start)]

  collated_ks2_data_pre2015 <- collated_ks2_data_pre2015[record_order == 1]

  collated_ks2_data_pre2015[, c("ks2_level4plus_passes",
                                "ks2_reading_points",
                                "ks2_maths_points",
                                "ks2_points",
                                "record_order") := NULL]


  # KS2 2015 onward

  collated_ks2_data[, ks2_passes := rowSums(.SD, na.rm = TRUE), .SDcols = c("ks2_reading_at_or_above_expected_level_2015_onward",
                                                                            "ks2_maths_at_or_above_expected_level_2015_onward")]

  collated_ks2_data[, ks2_high_passes := rowSums(.SD, na.rm = TRUE), .SDcols = c("ks2_reading_above_expected_level_2015_onward",
                                                                                 "ks2_maths_above_expected_level_2015_onward")]

  collated_ks2_data[, ':=' (ks2_maths_mark_int = as.integer(ks2_maths_mark_2015_onward),
                            ks2_reading_mark_int = as.integer(ks2_reading_mark))]

  collated_ks2_data[, ks2_total_marks := rowSums(.SD, na.rm = TRUE), .SDcols = c("ks2_maths_mark_int",
                                                                                 "ks2_reading_mark_int")]

  setorder(collated_ks2_data, -ks2_dfe_national_result_included, -ks2_passes, -ks2_high_passes, -ks2_total_marks)
  collated_ks2_data[, record_order := 1:.N, by = .(dfe_id, ks2_academic_year_start)]

  collated_ks2_data <- collated_ks2_data[record_order == 1]

  collated_ks2_data[, c("ks2_passes",
                        "ks2_high_passes",
                        "ks2_maths_mark_int",
                        "ks2_reading_mark_int",
                        "ks2_total_marks",
                        "record_order") := NULL]


  collated_ks2_data <- rbind(collated_ks2_data_pre2015,
                             collated_ks2_data)

  write_ks2_data_single_pupil_year <- writeParquetFile(collated_ks2_data,
                                                       dir_path = paste0(echoccs_data_directory_path,
                                                                         education_data_directory),
                                                       filename = "ks2_data")

  rm(collated_ks2_data,
     collated_ks2_data_pre2015)
  gc()


  # Key Stage 4 -------------------------------------------------------------

  ks4_common_fields <- c("KS4_PupilMatchingRefAnonymous",
                         "KS4_ACADYR",
                         "ks4_laestab_anon",
                         "KS4_ToE_CODE",
                         "KS4_NFTYPE",
                         "KS4_GENDER",
                         "KS4_YEAROFBIRTH",
                         "KS4_MONTHOFBIRTH",
                         "KS4_NATRES")

  sql_query_ks4 <- paste("SELECT",
                         "KS4_PupilMatchingRefAnonymous AS dfe_id,",
                         "CAST(SUBSTRING(KS4_ACADYR, 1, 4) AS SMALLINT) AS ks4_academic_year_start,",
                         "ks4_laestab_anon AS ks4_school_id,",
                         "KS4_ToE_CODE AS ks4_school_type_edubase,",
                         "KS4_NFTYPE AS ks4_school_type_aat,",
                         "KS4_GENDER AS ks4_gender,",
                         "KS4_YEAROFBIRTH AS ks4_birth_year,",
                         "KS4_MONTHOFBIRTH AS ks4_birth_month,",
                         "KS4_NATRES AS ks4_dfe_national_result_included,",
                         "ks4_level2,",
                         "ks4_best8_score,",
                         "ks4_english_grade,",
                         "ks4_english_points,",
                         "ks4_maths_grade,",
                         "ks4_maths_points",
                         "FROM (",
                         "SELECT",
                         paste0(ks4_common_fields,
                                collapse = ", "),
                         ",",
                         "CASE WHEN KS4_LEVEL2_EM IS NULL THEN KS4_LEVEL2_EM_PTQ ELSE KS4_LEVEL2_EM END AS ks4_level2,",
                         "CASE WHEN KS4_APENG_PTQ IS NULL THEN KS4_APENG ELSE KS4_APENG_PTQ END AS ks4_english_grade,",
                         "CASE WHEN KS4_HPGENG_PTQ IS NULL THEN KS4_HPGENG ELSE KS4_HPGENG_PTQ END AS ks4_english_points,",
                         "CASE WHEN KS4_HGMATH_PTQ IS NULL THEN KS4_HGMATH ELSE KS4_HGMATH_PTQ END AS ks4_maths_grade,",
                         "CASE WHEN KS4_HPGMATH_PTQ IS NULL THEN KS4_HPGMATH ELSE KS4_HPGMATH_PTQ END AS ks4_maths_points,",
                         "CASE WHEN KS4_PTSCNEWE_PTQ IS NULL THEN KS4_PTSCNEWE ELSE KS4_PTSCNEWE_PTQ END AS ks4_best8_score",
                         "FROM KS4Pupil_2002_to_2014",
                         "WHERE KS4_ACADYR >= '2012/2013'",
                         "UNION ALL SELECT",
                         paste0(ks4_common_fields,
                                collapse = ", "),
                         ",",
                         "CASE WHEN KS4_LEVEL2_EM_PTQ_EE IS NULL THEN KS4_LEVEL2_EM_94 ELSE KS4_LEVEL2_EM_PTQ_EE END AS ks4_level2,",
                         "CASE WHEN KS4_APENG_PTQ_EE IS NULL THEN KS4_APENG_91 ELSE KS4_APENG_PTQ_EE END AS ks4_english_grade,",
                         "KS4_HPGENG_PTQ_EE AS ks4_english_points,",
                         "CASE WHEN KS4_HGMATH_PTQ_EE IS NULL THEN KS4_HGMATH_91 ELSE KS4_HGMATH_PTQ_EE END AS ks4_maths_grade,",
                         "KS4_HPGMATH_PTQ_EE AS ks4_maths_points,",
                         "CASE WHEN KS4_ACADYR = '2014/2015' THEN KS4_PTSCNEWE_PTQ_EE ELSE KS4_ATT8 END AS ks4_best8_score",
                         "FROM KS4Pupil_2015_to_2021",
                         ") AS q1;")

  collated_ks4_data <- executeQuery(ms_conn, sql_query_ks4)
  setDT(collated_ks4_data)

  write_ks4_data_all <- writeParquetFile(collated_ks4_data,
                                         dir_path = paste0(echoccs_data_directory_path,
                                                           education_data_directory),
                                         filename = "ks4_data_all")

  # A sizeable number of pupils had more than one record within a given academic year.

  setorder(collated_ks4_data, -ks4_dfe_national_result_included, -ks4_level2, -ks4_best8_score)
  collated_ks4_data[, record_order := 1:.N, by = .(dfe_id, ks4_academic_year_start)]

  collated_ks4_data <- collated_ks4_data[record_order == 1]
  collated_ks4_data[, record_order := NULL]

  write_ks4_data_single_pupil_year <- writeParquetFile(collated_ks4_data,
                                                       dir_path = paste0(echoccs_data_directory_path,
                                                                         education_data_directory),
                                                       filename = "ks4_data")

  rm(collated_ks4_data)
  gc()


# Key Stage 5 -------------------------------------------------------------

  ks5_common_fields <- c("KS5_PupilMatchingRefAnonymous",
                         "KS5_ACADYR",
                         "KS5_NFTYPE",
                         "KS5_GENDER",
                         "KS5_YEAROFBIRTH",
                         "KS5_MONTHOFBIRTH",
                         "KS5_TOTPTSE_ALEV",
                         "KS5_GAS_MATH",
                         "KS5_GA_MATH")


  sql_query_ks5 <- paste("SELECT",
                         "KS5_PupilMatchingRefAnonymous AS dfe_id,",
                         "CAST(SUBSTRING(KS5_ACADYR, 1, 4) AS SMALLINT) AS ks5_academic_year_start,",
                         "ks5_laestab_anon AS ks5_school_id,",
                         "KS5_ToE_CODE AS ks5_school_type_edubase,",
                         "KS5_NFTYPE AS ks5_school_type_aat,",
                         "KS5_GENDER AS ks5_gender,",
                         "KS5_YEAROFBIRTH AS ks5_birth_year,",
                         "KS5_MONTHOFBIRTH AS ks5_birth_month,",
                         "KS5_TOTPTSE_ALEV AS ks5_total_points,",
                         "KS5_GAS_MATH AS ks5_aslevel_maths_grade,",
                         "KS5_GA_MATH AS ks5_alevel_maths_grade",
                         "FROM (",
                         "SELECT",
                         paste0(ks5_common_fields,
                                collapse = ", "),
                         ",",
                         "ks5_laestab_anon,",
                         "KS5_ToE_CODE",
                         "FROM KS5Student_2003_to_2021",
                         "WHERE KS5_ACADYR >= '2013/2014'",
                         "UNION ALL SELECT",
                         paste0(ks5_common_fields,
                                collapse = ", "),
                         ",",
                         "NULL AS ks5_laestab_anon,",
                         "NULL AS KS5_ToE_CODE",
                         "FROM KS5Student_2022",
                         ") AS q1;")

  collated_ks5_data <- executeQuery(ms_conn, sql_query_ks5)
  setDT(collated_ks5_data)

  # A sizeable number of pupils had more than one record within a given academic year.


  write_ks5_data <- writeParquetFile(collated_ks5_data,
                                     dir_path = paste0(echoccs_data_directory_path,
                                                       education_data_directory),
                                     filename = "ks5_data_all")

  setorder(collated_ks5_data, -ks5_total_points, ks5_alevel_maths_grade, ks5_aslevel_maths_grade)
  collated_ks5_data[, record_order := 1:.N, by = .(dfe_id, ks5_academic_year_start)]

  ks5_grades <- c("*",
                  LETTERS[1:5])

  collated_ks5_data[, ':=' (ks5_alevel_maths_grade = factor(ks5_alevel_maths_grade,
                                                            levels = ks5_grades,
                                                            ordered = TRUE),
                            ks5_aslevel_maths_grade = factor(ks5_aslevel_maths_grade,
                                                            levels = ks5_grades,
                                                            ordered = TRUE))]

  collated_ks5_data_summary <- copy(collated_ks5_data[record_order == 1, .(dfe_id,
                                                                           ks5_academic_year_start,
                                                                           ks5_school_id,
                                                                           ks5_school_type_edubase,
                                                                           ks5_school_type_aat,
                                                                           ks5_gender,
                                                                           ks5_birth_year,
                                                                           ks5_birth_month,
                                                                           ks5_total_points)])

  collated_ks5_data_summary <- merge(collated_ks5_data_summary,
                                     collated_ks5_data[, .(ks5_aslevel_maths_grade = min(ks5_aslevel_maths_grade, na.rm = TRUE),
                                                           ks5_alevel_maths_grade = min(ks5_alevel_maths_grade, na.rm = TRUE)),
                                                       by = .(dfe_id, ks5_academic_year_start)],
                                     by = c("dfe_id",
                                            "ks5_academic_year_start"))

  write_ks5_data <- writeParquetFile(collated_ks5_data_summary,
                                     dir_path = paste0(echoccs_data_directory_path,
                                                       education_data_directory),
                                     filename = "ks5_data")

  rm(collated_ks5_data, collated_ks5_data_summary)
  gc()


# Return result -----------------------------------------------------------


  if(!is.na(write_ks5_data) &
     !is.na(write_ks4_data_single_pupil_year) &
     !is.na(write_ks2_data_single_pupil_year) &
     !is.na(write_ks1_data) &
     !is.na(write_ks1_census_data)) {
    write_result <- TRUE
  } else {
    write_result <- as.logical(NA)
  }

  return(write_result)
}


