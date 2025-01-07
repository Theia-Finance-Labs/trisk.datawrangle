# This script generates the abcd input from asset_resolution data
# as well as the production_type.rda reference dataset in this packge
# TODO all the code before abcd_data should be translated to SQL

devtools::load_all()

data(scenarios_geographies)

# LOAD RAW Asset Impact ========================================


# parameters ========================================
path_ar_data_raw <-
  r2dii.utils::path_dropbox_2dii(
    "ST Inputs",
    "ST_INPUTS_PRODUCTION",
    "AR-Company-Indicators_2023Q4.xlsx"
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


# companies_with_zero_production <- abcd_stress_test_input %>%
#   dplyr::group_by(company_id, company_name, ald_business_unit, year) %>%
#   dplyr::summarise(plan_tech_prod=sum(plan_tech_prod)) %>%
#   dplyr::filter(year == min(year) & plan_tech_prod == 0) %>%
#   dplyr::distinct(company_id, company_name, ald_business_unit) %>%
#   dplyr::ungroup()

# num_companies_affected <- companies_with_zero_production %>%
#   dplyr::distinct(company_id) %>%
#   dplyr::count()

# # Calculate the number of affected companies per ald_business_unit
# affected_distribution <- companies_with_zero_production %>%
#   dplyr::group_by(ald_business_unit) %>%
#   dplyr::summarise(num_companies_affected = n_distinct(company_id), .groups = "drop") %>%
#   dplyr::arrange(dplyr::desc(num_companies_affected))

# # Calculate the total number of companies per ald_business_unit in the initial dataset
# total_distribution <- abcd_stress_test_input %>%
#   dplyr::group_by(ald_business_unit) %>%
#   dplyr::summarise(total_companies = n_distinct(company_id), .groups = "drop")

# # Merge the affected distribution with the total distribution
# merged_distribution <- affected_distribution %>%
#   dplyr::left_join(total_distribution, by = "ald_business_unit") %>%
#   dplyr::mutate(percentage_affected = (num_companies_affected / total_companies) * 100) %>%
#   dplyr::arrange(dplyr::desc(num_companies_affected))

# # Print the result
# print(merged_distribution)




# # Étape 1 : Identifier les ald_business_unit initiaux par entreprise
# initial_units_per_company <- abcd_stress_test_input %>%
#   dplyr::distinct(company_id, ald_business_unit)

# # Étape 2 : Supprimer les lignes affectées
# remaining_data <- abcd_stress_test_input %>%
#   dplyr::anti_join(companies_with_zero_production, by = c("company_id", "ald_business_unit", "country_iso2"))

# # Étape 3 : Identifier les ald_business_unit restants après suppression
# remaining_units_per_company <- remaining_data %>%
#   dplyr::distinct(company_id, ald_business_unit)

# # Étape 4 : Trouver les entreprises ayant perdu au moins un ald_business_unit
# companies_losing_any_units <- initial_units_per_company %>%
#   dplyr::anti_join(remaining_units_per_company, by = c("company_id", "ald_business_unit")) 


# # Résultats
# print(companies_losing_any_units)



# # Calculate the total production for each company
# total_production <- abcd_stress_test_input %>%
#   dplyr::group_by(company_id, company_name) %>%
#   dplyr::summarise(total_plan_tech_prod = sum(plan_tech_prod, na.rm = TRUE), .groups = "drop")

# # Filter companies that are in companies_with_zero_production
# filtered_production <- total_production %>%
#   dplyr::inner_join(companies_with_zero_production, by = c("company_id", "company_name")) %>%
#   dplyr::distinct(company_id, company_name, total_plan_tech_prod)

# # Get the top 15 companies based on total production
# top_15_companies <- filtered_production %>%
#   dplyr::arrange(dplyr::desc(total_plan_tech_prod)) %>%
#   dplyr::slice_head(n = 15) %>%
#   dplyr::select(company_name)

# # Print the result
# print(top_15_companies)


abcd_stress_test_input %>% readr::write_csv(output_path_stress_test_input)
