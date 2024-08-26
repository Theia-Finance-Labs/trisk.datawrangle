# This script generates the abcd input from asset_resolution data
# as well as the production_type.rda reference dataset in this packge
# TODO all the code before abcd_data should be translated to SQL

devtools::load_all()

data(scenarios_geographies)

# LOAD RAW Asset Impact ========================================


# parameters ========================================
path_ar_data_raw <-
  r2dii.utils::path_dropbox_2dii(
    "ST_INPUTS",
    "ST_INPUTS_PRODUCTION",
    "AR-Company-Indicators_2022Q4.xlsx"
  )


# following parameters are defined in workflow.R

# leave empty to use all countries
# country_filter <- c() 

# only use assets with HQ in the selected countries
# filter_hqs <- FALSE

# only use assets in the selected countries
# filter_assets <- FALSE
# parameters ========================================



outputs_list <- prepare_asset_impact_data(ar_data_path = path_ar_data_raw)
DB_company_activities <- outputs_list[["company_activities"]]
DB_company_emissions <- outputs_list[["company_emissions"]]


# Apply filterings dependant on company informations

company_informations <- read_asset_resolution(path_ar_data_raw,
                                              sheet_name = "Company Information")

# check that company/country pairs are uniques
company_informations %>%
  dplyr::group_by(company_id, ald_location) %>%
  dplyr::summarise(nrows = dplyr::n()) %>%
  dplyr::ungroup() %>%
  assertr::verify(max(nrows) == 1)

DB_company_activities <- DB_company_activities %>%
  filter_countries_coverage(
    company_informations = company_informations,
    country_filter = country_filter,
    filter_hqs = filter_hqs,
    filter_assets = filter_assets
  )

DB_company_emissions <- DB_company_emissions %>%
  filter_countries_coverage(
    company_informations = company_informations,
    country_filter = country_filter,
    filter_hqs = filter_hqs,
    filter_assets = filter_assets
  )


## TRANSFORM RAW Asset Impact to ABCD input ====================

# parameters ========================================
output_path_stress_test_input <-
  fs::path(
    "data-raw",
    "st_inputs",
  "abcd_stress_test_input",
  ext = "csv"
)
  

# start_year <- 2022 # defined in workflow.R
time_horizon <- 5
additional_year <- NULL
sector_list <- c("Automotive", "Power", "Oil&Gas", "Coal", "Steel")
# parameters ========================================

abcd_stress_test_input <-
  prepare_abcd_data(
    company_activities = DB_company_activities,
    company_emissions = DB_company_emissions,
    scenarios_geographies = scenarios_geographies, # loaded from package
    start_year = start_year,
    time_horizon = time_horizon,
    additional_year = additional_year,
    sector_list = sector_list
  )

abcd_stress_test_input %>% 
  assertr::verify(all(colSums(is.na(.)) == 0))

abcd_stress_test_input %>% readr::write_csv(output_path_stress_test_input)
