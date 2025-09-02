
getBirthsDeaths <- function(conn,
                            base_directory_path,
                            data_dir,
                            data_filename) {

  sql_query <- paste("SELECT",
                     "CASE WHEN births.TOKEN_PERSON_ID IS NOT NULL THEN",
                     "births.TOKEN_PERSON_ID ELSE deaths.TOKEN_PERSON_ID",
                     "END AS TOKEN_PERSON_ID,",
                     "ISNULL(birth_records, 0) AS birth_records,",
                     "birth_registration_date,",
                     "birth_sex,",
                     "ISNULL(birth_stillbirth_records, 0) AS birth_stillbirth_records,",
                     "ISNULL(death_records, 0) AS death_records,",
                     "death_date,",
                     "death_date_year_precision_only",
                     "FROM (",
                     "SELECT TOKEN_PERSON_ID,",
                     "MIN(CAST(CAST(CASE WHEN [DOREG] = '' THEN NULL ELSE [DOREG] END AS VARCHAR(10)) AS DATE))",
                     "AS birth_registration_date,",
                     "MIN(CAST([SEX_CHILD] AS VARCHAR(1))) AS birth_sex,",
                     "COUNT(*) AS birth_records,",
                     "SUM(CASE WHEN [STILLBIRTH_IND] = '1' THEN 1 ELSE 0 END)",
                     "AS birth_stillbirth_records",
                     "FROM [FILE0184779_civil_reg_births]",
                     "GROUP BY TOKEN_PERSON_ID",
                     ") AS births",
                     "FULL JOIN (",
                     "SELECT TOKEN_PERSON_ID,",
                     "MIN(CASE WHEN year_precision_only = 1 THEN",
                     "CAST(SUBSTRING(dod, 1, 4) +'0101' AS DATE)",
                     "ELSE CAST(dod AS DATE) END) AS death_date,",
                     "MAX(year_precision_only) AS death_date_year_precision_only,",
                     "COUNT(*) AS death_records",
                     "FROM (SELECT [TOKEN_PERSON_ID],",
                     "CAST([REG_DATE_OF_DEATH] AS VARCHAR(8)) AS dod,",
                     "CASE WHEN [REG_DATE_OF_DEATH] % 10000 = 0 THEN 1",
                     "ELSE 0 END AS year_precision_only",
                     "FROM [FILE0198796_NIC381972_civil_reg_deaths_resupply]",
                     ") AS deaths_sq",
                     "GROUP BY [TOKEN_PERSON_ID]",
                     ") AS deaths",
                     "ON (births.TOKEN_PERSON_ID = deaths.TOKEN_PERSON_ID);")

  births_deaths_records <- executeQuery(conn,
                                        sql_query)

  filepath <- writeParquetFile(births_deaths_records,
                               dir_path = paste0(base_directory_path,
                                                 data_dir),
                               filename = data_filename)

  return(filepath)
}
