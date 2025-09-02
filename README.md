# ECHO-CCS Data

This repo contains the data preparation code for the <u>E</u>du<u>c</u>ation 
and longer-term <u>H</u>ealth <u>O</u>utcomes for <u>C</u>hildhood 
<u>C</u>ancer <u>S</u>urvivors (ECHO-CCS) project. The `_main.r` file is the 
primary *runner*.

## Introduction

Cohort generation is non-trivial. Broadly, the files are prefixed with a 
number indicating the sequence in which they are run. A number of files 
start `00-`. These files contain utility functions: functions that are 
called for saving files or executing SQL queries.

## An overview

1.  **Stage 1:**
    1.  Create a long form table of all diagnoses in completed episodes 
        (`epistat` is `3`) across all the available HES APC data.
    1.  Collate all birth and death records and, for each represented TPI
        tokenised person ID (TPI), obtain:
        *  number of birth records (one might think precisely 1...)
        *  number of death records (one might think 1 at most...)
        *  earliest (valid) date of registration of birth (NB: date of birth is 
           not held within the available birth registration data)
        *  earliest (valid) month and year of date of death (NB: precise date of 
           death is not held within the available death registration data; a 
           very few records have only year level precision for date of death, 
           these are assigned to the (1st) January of the indicated year.)
        *  presence of any birth record indicating the infant was stillborn.
    1.  From the entirety of HES APC 1997-08 to 2022-23, collate all completed 
        episodes (records) with a TPI for which there exists one or more record 
        that indicates the patient with that TPI (in that/those record/s) was, 
        either:
        *  aged 0-28 days at the start or end the episode (`startage` or 
           `endage` with value `7001`, `7002`, or `7003`); or,
        *  their (valid: not empty, `1800-01-01` or `1801-01-01`) year and 
           month of birth was equal, within the same record, to the year and 
           month of any valid (defined as before) date recorded amongst the 
           fields: `admidate`, `epistart`, `epiend`, or `disdate`.
    1.  Create a one-to-one lookup between NHS IDs (tokenised person ID, TPI) 
        and DfE IDs (anonymised Pupil Matching Reference, aPMR). Some NHS 
        IDs were linked to more than one DfE ID. A variable 
        (`dfe_id_problematic`) indicates such cases. Note: In such instances, 
        only a single DfE ID was retained (the first when sorted alphabetically;
        given these IDs are cryptographically hashed this is, essentially, a 
        random ordering).
    1.  Collate the following details from all pupil records from all available
        spring Censuses, from academic year 2001/02 to 2021/22:
        *  DfE ID (anonymised Pupil Matching Reference, aPMR)
        *  record status
        *  academic year
        *  gender
        *  ethnic group
        *  ethnic group minor
        *  source of ethnicity
        *  free school meals eligibility
        *  first language
        *  enrolment status
        *  part-time indicator
        *  actual national curriculum year group
        *  Provision under the SEN Code of Practice
        *  Nature of primary SEN
        *  Nature of secondary SEN
        *  Indicator of member of a SEN Unit
        *  Index of Multiple Deprivation (IMD) measures year of publication
        *  Overall IMD rank (decile)
        *  Income component of IMD rank (decile)
        *  Income Deprivation Affecting Children Index (IDACI), supplemental index to IMD, rank (decile)
    1.  Collate the following details from all HES APC records belonging to all 
        mothers identified in the mother-baby linkage table:
        *  NHS ID (tokenised person ID, TPI)
        *  All dates relating to the episode (record) of care
        *  Patient classification (e.g. Ordinary admission, mother and babies
           using only delivery facilities)
        *  Type of episode (e.g. general, birth, delivery)
        *  Geography related details of the patient's place of residence (e.g.
            middle layer super output area [MSOA] - 2001 and 2011 censuses, 
            region of residence, 2004-based overall Index of Multiple 
            Deprivation)
    1.  Collate absence data from across DfE NPD Absence tables (from 2005/06). 
        The following are recorded:
        *  total number of records a pupil had within the first 5 half terms 
           of the academic year (see DfE documentation for why 5 half terms);
        *  sum of the total number of possible sessions within the first 5 half 
           terms of the academic year from across all records (this 
           occasionally yields an implausibly large number of sessions);
        *  sum of the total number of sessions for which the pupil was absent 
           within the first 5 half terms of the academic year from across all 
           records;
        *  sum of the total number of sessions for which the pupil was absent 
           due to illness or medical reasons within the first 5 half terms of 
           the academic year from across all records (from 2006/07);
        *  the previous 3 repeated but for the final half term of the academic 
           year (from 2012/13).
           
1. **Stage 2:**
    1.  Identify a birth cohort (adapted from Dr Ania Zylbersztejn's work for the 
        HOPE study): from the collated records of those with one or 
        more records in which they are aged 28 days or less, or occurs within 
        their (year and) month of birth:
          1.  Summarise the records identified by each TPI, identifying the 
              earliest admission (based on the earliest valid [not empty, 
              `1800-01-01` or `1801-01-01`] date recorded amongst the 
              following fields: `admidate`, `epistart`, `epiend`, or `disdate`) 
              and identifying from episodes within 28 days of that earliest 
              admission (based on the same date fields):
                *  Any evidence of a birth episode, i.e. any of the following:
                    * Any diagnosis (`diag_01` to `diag_20`) indicating a 
                      delivery (`Z37`) or birth (`Z38`). (NB: Delivery 
                      diagnoses are intended to be used on the mother's 
                      record only but are sometimes mistakenly used on the 
                      infant's record.)
                    *  Any Healthcare Resources Group (HRG) field (`hrgnhs`, 
                      `suscorehrg`, or `sushrg`) with a code indicative of 
                      neonatal care (HRG v3: `N01` to `N05`; HRG v4: 
                      `PB01` to `PB06`).
                    * An episode type (`epitype`) recorded as a birth 
                      episode or other birth event (`3` or `6`).
                    * A patient classification (`classpat`) recorded as 
                      "Mothers and babies using only delivery facilities" (`5`).
                    * An admission method (`admimeth`) recorded as one of:
                        *  *The birth of a baby in this Health Care Provider*
                           (`82`)
                        * *Baby born outside the Health Care Provider except when born at home as intended* (`83`)
                        *  *Baby born at home as intended (available since 2013/14)* (`2C`)
                    *  A relevant code (`0` to `4`) recorded in the Neonatal 
                              Level of Care (`neocare`) field.
                * Any diagnosis (`diag_01` to `diag_20`) indicating the 
                  termination of a pregnancy in the perinatal period affecting 
                  the foetus/newborn (`P96.4`).
                * Any evidence of a stillbirth, i.e. any of the following:
                    * Any diagnosis (`diag_01` to `diag_20`) indicating a 
                      foetal death (`P95`) or a delivery outcome involving 
                      the stillbirth of one or more infants (`Z37.1`, `Z37.3`, 
                      `Z37.6`, or `Z37.7`). (NB: Delivery diagnoses are 
                      intended to be used on the mother's record only but are 
                      sometimes mistakenly used on the infant's record.)
                    * A discharge method (`dismeth`) recorded as *Stillbirth* 
                      (`5`).
                    * A birth status (`birstat_1`) indicating a *Still birth* 
                      (`2` to `4`).
                * An indication of a singleton birth, i.e. any diagnosis 
                  (`diag_01` to `diag_20`) indicating a delivery outcome 
                  involving a single birth (`Z37.0` or `Z37.1`) or a birth 
                  outcome of singleton (`Z38.0`, `Z38.1`, or `Z38.2`).
                * An indication of a multiple birth, i.e. either of:
                    * any diagnosis (`diag_01` to `diag_20`) indicating a
                      delivery outcome involving multiple births (`Z37.2` 
                      to `Z37.7`) or a birth outcome of multiple (`Z38.3` 
                      to `Z38.8`)
                    * A Number of Babies field (`numbaby`) with a value 
                      indicative of multiple births (`2` to `6`).
            1.  Augment this summary data by TPI with the following information 
                from all records for each TPI (regardless of time period):
                * Earliest year and month of birth (`mydob`) recorded
                * Earliest recorded gender (`sex`)
                * Number of distinct (valid) year and month of births recorded
                * Number of distinct sexes (female or male) recorded
                * Presence of any episode for which no valid dates 
                  (`admidate`, `epistart`, `epiend`, or `disdate`) are recorded
            1.  Using TPI, link the summarised births and deaths registrations to 
                the summarised infant records data
          1.  Apply the following inclusion criteria for identified TPIs:
                1.  Presence of a birth episode between 1st April 1997 and 31st 
                    March 2023 (inclusive), in the same month as- or in the 
                    month immediately prior or immediately after- the recorded 
                    date of birth (within that episode).
          1.  Apply the following exclusion criteria for identified TPIs:
                1.  Any TPI for which there is any record with no valid dates 
                    (since we cannot reliably construct a timeline for these 
                    TPIs)
                1.  Any TPI for which there is more than one month and year of 
                    birth recorded in the HES APC data
                1.  Any TPI with HES APC admission recorded to have occurred 
                    at any time prior to the month immediately before the 
                    month and year of birth recorded in the HES APC data
                1.  More than one linked birth registration record
                1.  More than one linked death registration record
                1.  A date of birth registration pre-dating the month of birth 
                    (based on APC data)
                1.  A date of death pre-dating the month of birth (based on APC 
                    data).
                1.  Except where there is a linked birth notification or birth 
                    registration data indicating a live birth:
                      1.  Any indication of a termination of pregnancy in the 
                          perinatal period affecting the foetus or newborn.
                      1.  Any indication of stillbirth from the APC records
                1.  Except where there is a linked birth notification 
                    indicating a live birth, any indication of stillbirth from 
                    birth notification data
                1.  Any indication of stillbirth from birth registration data
                1.  The earliest recorded GOR of residence, within an admission
                    in the period from the month prior(!) to birth to 6 months 
                    after birth, indicates the infant was not resident in 
                    England.
    1.  Collate all cancer diagnoses. For each broad diagnosis group (Leukaemia, 
        CNS, Lymphoma, Other), identify the earliest diagnosis for each 
        TPI (as applicable for each group).
    1.  Collate KS1, 2, 4, and 5 information. KS2, 4, and 5 results (Exam) data 
        are routinely linked to pupil level school spring census data by DfE. 
        However, this is not the case for KS1 data. 
          * Using the collated pupil records from all available spring Censuses, 
            identify those Pupils in National Curriculum Year 2 and collate 
            their DfE ID (aPMR) and the academic year in which they were 
            recorded as being in National Curriculum Year 2.
          * For KS1, extract reading, writing and maths outcomes. Some pupils 
            had multiple records per academic year. In these instances, the 
            highest ordered record was selected per pupil per academic year in 
            which they appeared using the following ordering: number of subjects
            in which the pupil achieved Level 2 or above (descending order), 
            best combination of levels achieved across reading, writing and 
            maths (highest level first).
          * For KS2, extract reading and maths outcomes. Some pupils 
            had multiple records per academic year. In these instances, the 
            highest ordered record was selected per pupil per academic year in 
            which they appeared using the following ordering: whether DfE 
            included the recorded in the national results (true first), number 
            of subjects in which the pupil achieved Level 4 or above (descending 
            order), best combination of levels achieved across reading and maths 
            (highest level first).
          * For KS4, extract GCSE grades achieved for English and Maths and 
            whether the pupil achieved 5 or more Level 2 qualifications 
            (including English and Maths). Some pupils had multiple records per 
            academic year. In these instances, the highest ordered record was 
            selected per pupil per academic year in which they appeared using 
            the following ordering: whether DfE included the record in the 
            national results (true first), whether the pupil achieved 5 Level 2 
            qualifications including English and Maths (true first), best 
            combination of grades achieved across English and Maths (highest 
            level first).
          * For KS5, extract total points for all A Levels. Some pupils had multiple 
            records per academic year. In these instances, the highest ordered 
            record was selected per pupil per academic year in which they 
            appeared using the following ordering: total number of points 
            achieved at A Level. Additionally, the highest AS Level grade or/and 
            highest A Level grade achieved for Maths within that academic year 
            by the pupil was extracted.
    1.  Collate all diagnoses that may relate to a congenital abnormality based 
        on EUROCAT v1.5 definitions. Among these diagnoses, extract the earliest 
        episode date for the first such diagnosis relating to:
          * a congenital abnormality diagnosis that is excluded if it is 
            recorded for an infant born before 37 week gestation;
          * any other congenital abnormality;
          * the specific congenital abnormalities, individually, of:
              1. Down syndrome;
              1. neurofibromatosis; or,
              1. tuberous sclerosis.
1. **Stage 3:**
    1.  Collate birth information from available records for the birth cohort.
          1.  Specifically, of records with an earliest date recorded as occurring 
              within the person's month of birth or the month immediately preceding or
              succeeding that month, identify the earliest record with at least one of 
              the following fields containing a valid, non-missing value:
                * weeks gestation at birth (`gestat`);
                * birth weight (`birweit`); and
                * mother's age on the date of the infant's birth (`matage`).
          1.  Additional information is recorded to indicate potential issues with the 
              selected birth record (e.g. if there are multiple birth records recorded
              for the person prior to the month of their eighth birthday). 
      1.  Additionally, the same fields as listed above are extracted, for all 
              members of the birth cohort, where available, from the mother-baby 
              linkage data.
    1.  KS1, 2, 4, and 5 education data are combined for each individual, such 
        that outcome data from each Key Stage is included for, at most, one 
        academic year for each birth cohort member. The record chosen is that in 
        the latest (most recent) year up to and including the nominal year in 
        which the pupil would normally be expected to be entered for the 
        relevant test/exam based on the pupil's month and year of birth (e.g. a 
        person born in July 2000 would be expected to enter school in September 
        2004 and undertake their KS1 tests at the end of the academic year 
        2006/07. If they had, for whatever reason, a KS1 outcome for the years 
        2005/06, 2006/07 and 2007/08, only the 2006/07 record would be retained.)
    1.  Save ethnicity and geography-related information from available HES APC 
        records for the birth cohort. Amongst all HES APC records identify the 
        earliest record valid ethnicity and earliest recorded valid place of 
        residence derived geographic information (middle layer super output 
        area, region, and overall IMD [2004]). For these data items, 
        additionally record the earliest date of the record from which they are 
        obtained.
    1.  Save SEND and Absence data for each nominal school year (i.e. Y0 = the 
        academic year starting in the year in which the pupil turned 4 if born 
        before September, or the academic year starting in the year in which the 
        pupil turned 5 if born in September or later)
          1.  If more than one (spring) census record was recorded for a pupil 
              within an academic year, the records were order by decreasing severity 
              of SEN provision [EHCP, SEN support, Statement, School Action Plus 
              and statutory assessment, School Action Plus, School Action, No SEN], 
              the record status (DfE's [imperfect] identification of duplicate 
              records), enrolment status type [in order: Current, Current Main, 
              Current Subsidiary, FE College, Other provider, Guest, NULL]. The 
              first record was chosen based on the above ordering for each pupil and 
              academic year, yielding: 
                * SEN status/provision;
                * Primary SEN type (since 2003/04); and
                * Secondary SEN type (since 2003/04).
          1.  Additionally, from all records for each pupil and academic year, 
              if any record indicated special provision this was recorded.
          1.  Absence data, as described within Stage 1, was incorporated.
1. **Stage 4:**
      1.  Compile the all the above data together for each member of the birth 
          cohort. To help with interpreting the complete cohort data, some fields 
          / variables are prefixed with text indicating from whence that field 
          / variable was derived:
            * `birth_` - (ONS supplied) birth registrations
            * `death_` - (ONS supplied) death registrations
            * `notification_` - NHS birth notifications (originally from NHS 
              Number for Babies [NN4B] service, subsequently via a component [or 
              interface] of the Personal Demographics Service [PDS])
            * `dfe_` - DfE ID (anonymised Pupil Matching Reference, aPMR) to NHS 
              ID (tokenised person ID, TPI) lookup, generated by NHS England for 
              ECHILD v2.
            * `bi_` - Birth information, based on earliest record for members of 
              the birth cohort.
            * `mb_` - Data derived from the Mother-Baby linkage data (only 
              includes hospital deliveries/births until to end March 2022).
            * `mi_` - Mothers' information, based on mothers' (identified from 
              Mother-Baby linkage data) episodes of care occurring within (or 
              intersecting) the month of birth of the respective cohort member.
            * `eth_` - Ethnicity information derived from all available HES APC
              records belonging to the respective cohort member.
            * `geo_` - Geography-related data derived from recorded place of 
              residence from all available HES APC records belonging to the 
              respective cohort member.
            * `cancer_` - Cancer diagnosis related data derived from all available 
              HES APC records belonging to the respective cohort member.
            * `ca_` - Congenital abnormality diagnosis related data derived from 
              all available HES APC records belonging to the respective cohort 
              member.
            * `census_` - Derived from all available pupil-level spring school 
              census data relating to pupils who were taught, for the majority of 
              their time within the given academic year, in Year 2 (i.e. the Year 
              in which most pupils would be expected to take their KS1 tests).
            * `ks1_` - Derived from KS1 data.
            * `ks2_` - Derived from KS2 data.
            * `ks4_` - Derived from KS4 data.
            * `ks5_` - Derived from KS5 data.
            * `school_start_year` - Expected year in which the cohort member would 
              have been expected to start school, based on their year and month of
              birth (i.e. the year in which they turn 4 years old if born before
              September, otherwise the year in which they turn 5 years old.)
            * `abs_` - Derived from Absence data.
            * `sen_` - Derived from pupil-level spring census data relating to 
              SEN.
            * `senabs_` - Common to both `abs_`/`sen_`.
            * `echoccs_cohort` - `TRUE` if they meet study cohort eligibility 
              criteria, `FALSE` otherwise.
1. **Stage 5:**
      1.  Read in the `echoccs_cohort` (saved as a Parquet file type).
      1.  Write the `echoccs_cohort` as a comma separated values (CSV) text file 
          type.
