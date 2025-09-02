createEchoccsBirthCohort <- function(conn,
                                     echoccs_data_directory_path,
                                     birth_cohort_name,
                                     mothers_information_name,
                                     cancer_diagnoses_name,
                                     congenital_diagnoses_name,
                                     birth_information_name,
                                     education_data_directory,
                                     education_data_combined_name,
                                     ethnicity_geo_name,
                                     absence_send_combined_name,
                                     echoccs_cohort_name) {

  # conn <- duckdb_conn
  # cohort_name <- echoccs_cohort_name

  birth_cohort_filepath <- paste0(echoccs_data_directory_path,
                                  birth_cohort_name,
                                  "/",
                                  birth_cohort_name,
                                  ".parquet")

  birth_information_filepath <- paste0(echoccs_data_directory_path,
                                       birth_information_name,
                                       "/",
                                       birth_information_name,
                                       ".parquet")

  mothers_information_filepath <- paste0(echoccs_data_directory_path,
                                         mothers_information_name,
                                         "/",
                                         mothers_information_name,
                                         ".parquet")

  cancer_diagnoses_filepath <- paste0(echoccs_data_directory_path,
                                      cancer_diagnoses_name,
                                      "/",
                                      cancer_diagnoses_name,
                                      ".parquet")

  congenital_diagnoses_filepath <- paste0(echoccs_data_directory_path,
                                      congenital_diagnoses_name,
                                      "/",
                                      congenital_diagnoses_name,
                                      ".parquet")

  education_data_filepath <- paste0(echoccs_data_directory_path,
                                    education_data_directory,
                                    "/",
                                    education_data_combined_name,
                                    ".parquet")

  ethnicity_geo_data_filepath <- paste0(echoccs_data_directory_path,
                                        ethnicity_geo_name,
                                    "/",
                                    ethnicity_geo_name,
                                    ".parquet")

  absence_sen_data_filepath <- paste0(echoccs_data_directory_path,
                                    education_data_directory,
                                    "/",
                                    absence_send_combined_name,
                                    ".parquet")

  # Link birth cohort to birth info -----------------------------------------

  birth_information_fields <- c('token_person_id',
                                'bi_valid',
                                'bi_admidate',
                                'bi_episode_min_date',
                                'bi_records_before_8yrs',
                                'bi_gestat',
                                'bi_birweit',
                                'bi_matage',
                                'bi_sexbaby',
                                'bi_imd04_decile',
                                'mb_token_person_id_mother',
                                'mb_matage',
                                'mb_gestat',
                                'mb_birweit',
                                'mb_sexbaby',
                                'mb_imd04_decile',
                                'mb_mdel_my')

  sql_birth_cohort_birth_info_link <- paste("DROP TABLE IF EXISTS birth_cohort;",
                                            "CREATE TABLE birth_cohort AS (SELECT",
                                            "bc.*,",
                                            paste0("bi.",
                                                   birth_information_fields[-1],
                                                   collapse = ", "),
                                            "FROM",
                                            paste0("read_parquet('",
                                                   birth_cohort_filepath,
                                                   "')"),
                                            "AS bc",
                                            "LEFT JOIN",
                                            paste0("read_parquet('",
                                                   birth_information_filepath,
                                                   "')"),
                                            "AS bi ON",
                                            "bc.token_person_id = bi.token_person_id);")

  res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_birth_info_link,
                      query_type = "SEND")


  # Link birth cohort to mothers info ---------------------------------------

  mothers_information_fields <- c('mi_token_person_id_mother',
                                  'mi_mdel_my',
                                  'mi_msoa01',
                                  'mi_msoa11',
                                  'mi_imd04_decile',
                                  'mi_resgor',
                                  'mi_cr_residence',
                                  'mi_episode_min_date',
                                  'mi_episode_max_date')

  sql_birth_cohort_mothers_info_link <- paste("DROP TABLE IF EXISTS birth_cohort_mi;",
                                              "CREATE TABLE birth_cohort_mi AS (SELECT",
                                              "bc.*,",
                                              paste0("mi.",
                                                     mothers_information_fields[-(1:2)],
                                                     collapse = ", "),
                                              "FROM birth_cohort AS bc",
                                              "LEFT JOIN",
                                              paste0("read_parquet('",
                                                     mothers_information_filepath,
                                                     "')"),
                                              "AS mi ON",
                                              "bc.mb_token_person_id_mother = mi.mi_token_person_id_mother AND",
                                              "bc.mb_mdel_my = mi.mi_mdel_my);",
                                              "DROP TABLE IF EXISTS birth_cohort;")

  res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_mothers_info_link,
                      query_type = "SEND")


  # Link birth cohort to ethnicity and geo info -----------------------------

  ethnicity_geo_fields <- c("eth_ethnicity_broad",
                            "eth_ethnicity_coding_system",
                            "eth_months_since_dob",
                            "eth_ethnicity_broad_n",
                            "geo_oacode6",
                            "geo_msoa01",
                            "geo_msoa11",
                            "geo_months_since_dob")

  sql_birth_cohort_mothers_info_link <- paste("DROP TABLE IF EXISTS birth_cohort_eth_geo;",
                                              "CREATE TABLE birth_cohort_eth_geo AS (SELECT",
                                              "bc.*,",
                                              paste0("eth.",
                                                     ethnicity_geo_fields,
                                                     collapse = ", "),
                                              "FROM birth_cohort_mi AS bc",
                                              "LEFT JOIN",
                                              paste0("read_parquet('",
                                                     ethnicity_geo_data_filepath,
                                                     "')"),
                                              "AS eth ON",
                                              "bc.token_person_id = eth.token_person_id);",
                                              "DROP TABLE IF EXISTS birth_cohort_mi;")

  res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_mothers_info_link,
                      query_type = "SEND")

  # Link birth cohort to cancer diagnoses -----------------------------------

  cancer_col_names <- c("group",
                        "earliest_diagnosis",
                        "earliest_diagnosis_problematic")

  cancer_diag_fields <- paste0("cancer_",
                               rep(cancer_col_names, 4),
                               "_",
                               rep(1:4, each = length(cancer_col_names)))

  sql_birth_cohort_cancer_diags_link <- paste("DROP TABLE IF EXISTS birth_cohort_cd;",
                                              "CREATE TABLE birth_cohort_cd AS (SELECT",
                                              "bc.*,",
                                              paste0("cd.",
                                                     cancer_diag_fields,
                                                     collapse = ", "),
                                              "FROM birth_cohort_eth_geo AS bc",
                                              "LEFT JOIN",
                                              paste0("read_parquet('",
                                                     cancer_diagnoses_filepath,
                                                     "')"),
                                              "AS cd ON",
                                              "bc.token_person_id = cd.token_person_id);",
                                              "DROP TABLE IF EXISTS birth_cohort_mi;")

  res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_cancer_diags_link,
                      query_type = "SEND")




# Link birth cohort to congenital abnormality diagnoses -------------------

  ca_fields <- c("ca_down_syndrome",
                 "ca_neurofibromatosis",
                 "ca_tuberous_sclerosis",
                 "ca_other",
                 "ca_ga_mte_37wks_only")

  sql_birth_cohort_congenital_diags_link <- paste("DROP TABLE IF EXISTS birth_cohort_ca;",
                                              "CREATE TABLE birth_cohort_ca AS (SELECT",
                                              "bc.*,",
                                              paste0("ca.",
                                                     ca_fields,
                                                     collapse = ", "),
                                              "FROM birth_cohort_cd AS bc",
                                              "LEFT JOIN",
                                              paste0("read_parquet('",
                                                     congenital_diagnoses_filepath,
                                                     "')"),
                                              "AS ca ON",
                                              "bc.token_person_id = ca.token_person_id);",
                                              "ALTER TABLE birth_cohort_ca RENAME COLUMN token_person_id TO nhs_id;",
                                              "ALTER TABLE birth_cohort_ca RENAME COLUMN mb_token_person_id_mother TO mb_nhs_id_mother;",
                                              "DROP TABLE IF EXISTS birth_cohort_cd;")

  res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_congenital_diags_link,
                      query_type = "SEND")


# Link absence / sen combined data ----------------------------------------

  senabs_per_yr_fields <- c('senabs_academic_year_start_Y',
                            'sen_NC_year_actual_Y',
                            'sen_status_Y',
                            'sen_primary_type_Y',
                            'sen_secondary_type_Y',
                            'sen_any_special_provision_Y',
                            'abs_records_Y',
                            'abs_annual_possible_Y',
                            'abs_annual_absent_total_Y',
                            'abs_summed_possible_Y',
                            'abs_summed_absent_total_Y',
                            'abs_summed_absent_illness_medical_Y',
                            'abs_summer6th_possible_Y',
                            'abs_summer6th_absent_total_Y',
                            'abs_summer6th_absent_illness_medical_Y')

  senabs_yrs <- sprintf("%02d", -2:13)

  senabs_fields <- paste0("\"",
                          rep(senabs_per_yr_fields, length(senabs_yrs)),
                          rep(senabs_yrs, each = length(senabs_per_yr_fields)),
                          "\"")

  sql_birth_cohort_sen_absence_link <- paste("DROP TABLE IF EXISTS birth_cohort_senabs;",
                                                  "CREATE TABLE birth_cohort_senabs AS (SELECT",
                                                  "bc.*,",
                                                  paste0("ca.",
                                                         senabs_fields,
                                                         collapse = ", "),
                                                  "FROM birth_cohort_ca AS bc",
                                                  "LEFT JOIN",
                                                  paste0("read_parquet('",
                                                         absence_sen_data_filepath,
                                                         "')"),
                                                  "AS ca ON",
                                                  "bc.dfe_id = ca.dfe_id);",
                                                  "DROP TABLE IF EXISTS birth_cohort_ca;")


    res <- executeQuery(conn,
                      sql_query = sql_birth_cohort_sen_absence_link,
                      query_type = "SEND")

    rm(senabs_per_yr_fields,
       senabs_yrs,
       senabs_fields)


# Link combined education data --------------------------------------------

  ks_fields <- c('census_academic_year_start',
                 'ks1_school_id',
                 'ks1_academic_year_start',
                 'ks1_reading_1997_2014',
                 'ks1_reading_2015_2021',
                 'ks1_writing_1997_2014',
                 'ks1_writing_2015_2021',
                 'ks1_maths_1997_2014',
                 'ks1_maths_2015_2021',
                 'ks2_academic_year_start',
                 'ks2_school_id',
                 'ks2_dfe_national_result_included',
                 'ks2_reading_mark',
                 'ks2_reading_1998_2014',
                 'ks2_reading_2015_onward',
                 'ks2_reading_above_expected_level_2015_onward',
                 'ks2_reading_at_or_above_expected_level_2015_onward',
                 'ks2_maths_mark_1995_2014',
                 'ks2_maths_mark_2015_onward',
                 'ks2_maths_1995_2014',
                 'ks2_maths_2015_onward',
                 'ks2_maths_above_expected_level_2015_onward',
                 'ks2_maths_at_or_above_expected_level_2015_onward',
                 'ks4_academic_year_start',
                 'ks4_school_id',
                 'ks4_dfe_national_result_included',
                 'ks4_level2',
                 "ks4_best8_score",
                 "ks4_english_grade",
                 "ks4_english_points",
                 "ks4_maths_grade",
                 "ks4_maths_points",
                 'ks5_academic_year_start',
                 'ks5_school_id',
                 'ks5_total_points',
                 'ks5_aslevel_maths_grade',
                 'ks5_alevel_maths_grade')

  echoccs_cohort <- executeQuery(conn,
                                 sql_query = paste("SELECT",
                                                   "bc.*,",
                                                   paste0("ks.",
                                                          ks_fields,
                                                          collapse = ", "),
                                                   "FROM birth_cohort_senabs AS bc",
                                                   "LEFT JOIN",
                                                   paste0("read_parquet('",
                                                          education_data_filepath,
                                                          "')"),
                                                   "AS ks",
                                                   "ON bc.dfe_id = ks.dfe_id;"))

  setDT(echoccs_cohort)

  res <- executeQuery(conn,
                      sql_query = paste("DROP TABLE birth_cohort_senabs;"),
                      query_type = "SEND")
  gc()

# Derive some further variables -------------------------------------------

  school_year_breaks <- c(-Inf, 2, 6, 11, Inf)
  age_breaks <- c(0, 1, seq(5, 25, by = 5), Inf)

# Residency in England based on mother's record
  echoccs_cohort[mi_resgor %in% c('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K'), mi_resgor_england := 1L]
  echoccs_cohort[mi_resgor %in% c('S', 'W', 'X', 'Z'), mi_resgor_england := 0L]
  echoccs_cohort[is.na(mi_resgor_england) & nchar(mi_cr_residence) == 3 & mi_cr_residence != "Q99", mi_resgor_england := 1L]
  echoccs_cohort[is.na(mi_resgor_england) & mi_cr_residence %in% c("Q99", 'S', 'X', 'Z'), mi_resgor_england := 0L]

  # Nominal school start year
  echoccs_cohort[, school_start_year := year(min_dob) + 4L + as.integer(month(min_dob) >= 9)]

  # Age at earliest diagnosis
  echoccs_cohort[, cancer_earliest_diagnosis_1_age := lubridate::as.period(lubridate::interval(start = min_dob, end = cancer_earliest_diagnosis_1))$year]
  echoccs_cohort[, cancer_earliest_diagnosis_1_age_cat := cut(cancer_earliest_diagnosis_1_age,
                                                              breaks = age_breaks,
                                                              right = FALSE,
                                                              ordered_result = TRUE)]

  # Nominal school year at earliest diagnosis
  echoccs_cohort[, cancer_earliest_diagnosis_1_school_year := lubridate::year(cancer_earliest_diagnosis_1) - school_start_year - as.integer(lubridate::month(cancer_earliest_diagnosis_1) < 7)]
  echoccs_cohort[, cancer_earliest_diagnosis_1_school_year_cat := cut(cancer_earliest_diagnosis_1_school_year,
                                                                               breaks = school_year_breaks,
                                                                               right = TRUE,
                                                                               ordered_result = TRUE)]

  # Age at death
  echoccs_cohort[, death_age := lubridate::as.period(lubridate::interval(start = min_dob, end = death_date))$year]
  echoccs_cohort[, death_age_cat := cut(death_age,
                                        breaks = age_breaks,
                                        right = FALSE,
                                        ordered_result = TRUE)]

  # Nominal school year at death
  echoccs_cohort[, death_school_year := lubridate::year(death_date) - school_start_year - as.integer(lubridate::month(death_date) < 7)]
  echoccs_cohort[, death_school_year_cat := cut(death_school_year,
                                                breaks = school_year_breaks,
                                                right = TRUE,
                                                ordered_result = TRUE)]

  # Define cohort
  echoccs_cohort[any_birth_record == 1 &
                   min_dob >= as.Date('1997-04-01') & min_dob < as.Date('2023-04-01') &
                   any_missing_episode_min_date == 0 &
                   n_dob == 1 &
                   min_months_after_dob > -2 &
                   (is.na(birth_records) | birth_records < 2) &
                   (is.na(death_records) | death_records < 2) &
                   (is.na(birth_registration_date) | birth_registration_date >= min_dob) &
                   (is.na(birth_registration_date) | is.na(notification_dob) | birth_registration_date >= notification_dob) &
                   (is.na(death_date) | death_date >= min_dob | (death_date_year_precision_only == 1 & lubridate::`%m+%`(death_date, months(12)) > min_dob)) &
                   ((any_termination_record == 0 & any_stillbirth_record == 0 & is.na(birth_stillbirth_records) & (is.na(notification_stillbirth) | notification_stillbirth == '')) | (notification_stillbirth %in% c('1', '5') & is.na(birth_stillbirth_records)) | birth_stillbirth_records == 0) &
                   (is.na(resgor_england_months_since_min_dob) | resgor_england_months_since_min_dob > 6 | (resgor_england_months_since_min_dob <= 6 & resgor_england == 1)) &
                   (is.na(mi_resgor_england) | mi_resgor_england == 1),
                 echoccs_cohort := TRUE]
  echoccs_cohort[is.na(echoccs_cohort), echoccs_cohort := FALSE]

  # Save cohort
  setDF(echoccs_cohort)
  write_echoccs_cohort <- writeParquetFile(echoccs_cohort,
                                           dir_path = paste0(echoccs_data_directory_path,
                                                             echoccs_cohort_name),
                                           filename = echoccs_cohort_name)

  rm(echoccs_cohort)
  gc()

  return(write_echoccs_cohort)
}


