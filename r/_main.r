# Libraries, fns, etc. ----------------------------------------------------

library(data.table)
library(arrow)
library(duckdb)

source('r/00-arrow_fns.r')
source('r/00-db_fns.r')

source('r/01a-collateDiagnoses.r')
source('r/01b-getBirthsDeaths.r')
source('r/01c-collateInfantsRecords.r')
source('r/01d-getNhsToDfeIdLookup.r')
source('r/01e-collateEduCensusData.r')
source('r/01f-saveMothersInfo.r')
source('r/01g-collateAbsenceData.r')

source('r/02a-createBirthCohort.r')
source('r/02b-collateCancerDiagnoses.r')
source('r/02c-collateEduKSData.r')
source('r/02d-collateCongenitalDiagnoses.r')

source('r/03a-saveBirthInfo.r')
source('r/03b-combineEducationData.r')
source('r/03c-saveEthnicityGeoInfo.r')
source('r/03d-combineAbsenceSendData.r')

source('r/04-createEchoccsBirthCohort.r')
source('r/05-writeEchoccsCohortAsCsv.r')

# CONFIG ------------------------------------------------------------------

source("r/_config.r")


# Notes -------------------------------------------------------------------

warning_msgs <- NULL
for(msg in warning_msgs)  warning(msg)



# DB connections ----------------------------------------------------------

mssql_conn <- getDBConn(db_conn_details)

duckdb_conn <- dbConnect(duckdb(),
                      dbdir = ":memory:")



# Processing: Stage 1 -----------------------------------------------------
# (i.e. not reliant on any prior processing stages)

# Get HES APC table names
## This take a long time to run so skip unless first time
if(TRUE) {
  hes_apc_tables <- c('FILE0184780_HES_APC_1997', 'FILE0184781_HES_APC_1998',
                      'FILE0184782_HES_APC_1999', 'FILE0184783_HES_APC_2000',
                      'FILE0184784_HES_APC_2001', 'FILE0184785_HES_APC_2002',
                      'FILE0184786_HES_APC_2003', 'FILE0184788_HES_APC_2004',
                      'FILE0184790_HES_APC_2005', 'FILE0184793_HES_APC_2006',
                      'FILE0184794_HES_APC_2007', 'FILE0184797_HES_APC_2008',
                      'FILE0184801_HES_APC_2009', 'FILE0184805_HES_APC_2010',
                      'FILE0184809_HES_APC_2011', 'FILE0184813_HES_APC_2012',
                      'FILE0184817_HES_APC_2013', 'FILE0184822_HES_APC_2014',
                      'FILE0184830_HES_APC_2015', 'FILE0184835_HES_APC_2016',
                      'FILE0184838_HES_APC_2017', 'FILE0184849_HES_APC_2018',
                      'FILE0184861_HES_APC_2019', 'FILE0184866_HES_APC_2020',
                      'FILE0184876_HES_APC_2021', 'FILE0184883_HES_APC_2022')
} else {
  hes_apc_tables <- getEchildTableNames(mssql_conn,
                                        "HES_APC",
                                        get_row_counts = FALSE)
}


# Collate all diagnoses across all HES APC files
#   & -separately- Births and Deaths registrations

## This take a long time to run so skip unless first time
if(TRUE) {
  diagnoses_filepath <- paste0(echildv2_data_directory_path,
                               diagnoses_data_directory)

  births_deaths_filepath <- paste0(echoccs_data_directory_path,
                                   births_deaths_name, "/",
                                   births_deaths_name, ".parquet")
} else {
  diagnoses_filepath <- collateDiagnoses(mssql_conn,
                                         hes_apc_tables,
                                         echildv2_data_directory_path,
                                         diagnoses_data_directory)

  births_deaths_filepath <- getBirthsDeaths(mssql_conn,
                                            echoccs_data_directory_path,
                                            births_deaths_name)
}

if(is.na(diagnoses_filepath))
  stop("Failed to collate diagnoses.")


if(is.na(births_deaths_filepath))
  stop("Failed to write births/deaths data.")


# Collate Infant Records
infants_records_filepath <- collateInfantsRecords(mssql_conn,
                                                  hes_apc_tables,
                                                  echoccs_data_directory_path,
                                                  infants_records_name)

if(is.na(infants_records_filepath))
  stop("Failed to collate infant records.")


# Construct NHS-to-DfE ID lookup
nhs_dfe_lookup_filepath <- getNhsToDfeIdLookup(mssql_conn,
                                               echoccs_data_directory_path,
                                               nhs_dfe_id_lookup_name)

if(is.na(nhs_dfe_lookup_filepath))
  stop("Failed to collate NHS to DfE ID lookup.")

# Collate School Census data
edu_census_filepath <- collateEduCensusData(mssql_conn,
                                            echoccs_data_directory_path,
                                            education_data_directory,
                                            edu_census_name)

if(is.na(edu_census_filepath))
  stop("Failed to collate school Spring census data.")

# Collate mothers delivery info
mothers_info_filepath <- saveMothersInfo(ms_conn,
                                         duckdb_conn,
                                         hes_apc_tables,
                                         echoccs_data_directory_path,
                                         mothers_information_name)

if(is.na(mothers_info_filepath))
  stop("Failed to collate mothers' delivery information.")


absence_data_filepath <- collateEduAbsenceData(ms_conn,
                                               echoccs_data_directory_path,
                                               education_data_directory,
                                               edu_absence_name,
                                               edu_absence_consolidated_name)

if(is.na(absence_data_filepath))
  stop("Failed to collate absence data.")

# Processing: Stage 2 -----------------------------------------------------
# (i.e. reliant on processing within Stage 1)

# Create birth cohort (takes over an hour to run)
birth_cohort_filepath <- createBirthCohort(duckdb_conn,
                                           mssql_conn,
                                           echoccs_data_directory_path,
                                           infants_records_name,
                                           births_deaths_name,
                                           nhs_dfe_id_lookup_name,
                                           birth_cohort_name)

if(is.na(birth_cohort_filepath))
  stop("Failed to create birth cohort.")

# Collate cancer diagnoses
cancer_diagnoses_filepath <- collateCancerDiagnoses(duckdb_conn,
                                                    echoccs_data_directory_path,
                                                    diagnoses_filepath,
                                                    cancer_diagnoses_name)
if(is.na(cancer_diagnoses_filepath))
  stop("Failed to collate cancer diagnoses.")


# Collate Y2 spring census presence, and Key Stage 1,2, and 4 data
education_data_filepath <- collateEduKSData(mssql_conn,
                                            duckdb_conn,
                                            echoccs_data_directory_path,
                                            edu_census_path,
                                            education_data_directory)

if(is.na(education_data_filepath))
  stop("Failed to collate education data.")

# Collate congenital abnormality diagnoses
congenital_diagnoses_filepath <- collateCongenitalDiagnoses(duckdb_conn,
                                                            echoccs_data_directory_path,
                                                            diagnoses_filepath,
                                                            congenital_diagnoses_name)
if(is.na(congenital_diagnoses_filepath))
  stop("Failed to collate congenital diagnoses.")


# Processing: Stage 3 -----------------------------------------------------
# (i.e. reliant on processing within Stage 2)

# get Birth Episode Info
birth_info_filepath <- saveBirthInfo(duckdb_conn,
                                     mssql_conn,
                                     echoccs_data_directory_path,
                                     infants_records_name,
                                     birth_information_name)

if(is.na(birth_info_filepath))
  stop("Failed to save birth information.")

education_data_combined_filepath <- combineEducationData(duckdb_conn,
                                                         echoccs_data_directory_path,
                                                         education_data_directory,
                                                         birth_cohort_name,
                                                         education_data_combined_name)

if(is.na(education_data_combined_filepath))
  stop("Failed to save combine education data.")


ethnicity_geo_filepath <- saveEthnicityGeoInfo(duckdb_conn,
                                               echoccs_data_directory_path,
                                               infants_records_name,
                                               ethnicity_geo_name)

if(is.na(ethnicity_geo_filepath))
  stop("Failed to save ethnicity and MSOA information.")


absence_send_filepath <- combineAbsenceSendData <- function(duckdb_conn,
                                                             echoccs_data_directory_path,
                                                             education_data_directory,
                                                             birth_cohort_name,
                                                             absence_send_combined_name)

if(is.na(absence_send_filepath))
  stop("Failed to save combined Absence / SEND information.")

# Processing: Stage 4 -----------------------------------------------------
# (i.e. reliant on processing within Stage 3)

echoccs_cohort_filepath <- createEchoccsBirthCohort(duckdb_conn,
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
                                                    echoccs_cohort_name)

if(is.na(echoccs_cohort_filepath))
  stop("Failed to create ECHOCCS cohort.")



DBI::dbDisconnect(mssql_conn)
DBI::dbDisconnect(duckdb_conn,
                  shutdown = TRUE)
gc()


writeEchoccsCohortAsCsv(echoccs_data_directory_path,
                        echoccs_cohort_name)
