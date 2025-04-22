library(dplyr)
options(r2dii_dropbox=r2dii_dropbox)

start_year <- 2023

# OPEN SOURCE DATA

print("=================== RUNNING run_prepare_Scenarios_AnalysisInput ===================")
source(fs::path("data-raw", "run_prepare_Scenarios_AnalysisInput.R"))
rm(list = ls()[ls() != "start_year"])

print("=================== RUNNING run_prepare_prewrangled_capacity_factors ===================")
source(fs::path("data-raw", "run_prepare_prewrangled_capacity_factors.R"))
rm(list = ls()[ls() != "start_year"])

print("=================== RUNNING run_prepare_price_data_long ===================")
source(fs::path("data-raw", "run_prepare_price_data_long.R"))
rm(list = ls()[ls() != "start_year"])

print("=================== RUNNING run_prepare_ngfs_carbon_price ===================")
source(fs::path("data-raw", "run_prepare_ngfs_carbon_price.R"))
rm(list = ls()[ls() != "start_year"])

print("=================== RUNNING run_rename_geographies ===================")
source(fs::path("data-raw", "run_rename_geographies.R"))
rm(list = ls()[ls() != "start_year"])



# ALIGN OPEN SOURCE DATAFRAMES TOGETHER

st_input_folder <- here::here("data-raw", "st_inputs") # TODO USE IN ALL OTHER SCRIPTS

capacity_factors_power <- readr::read_csv(
  fs::path(st_input_folder, "prewrangled_capacity_factors.csv"))
df_price <- readr::read_csv(
  fs::path(st_input_folder, "price_data_long.csv")) %>% dplyr::select(-c(scenario_geography)) %>% dplyr::distinct_all()
scenario_data <- readr::read_csv(
  fs::path(st_input_folder, "Scenarios_AnalysisInput.csv"))

scenario_price <- scenario_data %>%
  dplyr::inner_join(
    df_price,
    by = c("scenario", "ald_sector", "ald_business_unit", "year"),
    relationship="many-to-one" # many to one expected bc prices are not geography scpecific (Global)
    )

scenario_price_power <- scenario_price %>% dplyr::filter(ald_sector=="Power")
scenario_price_power_not_in_capfac <- scenario_price_power %>%
  dplyr::anti_join(
    capacity_factors_power ,
    by = c("scenario_geography", "scenario", "ald_business_unit"))

# remove from the scenarios+geographies which ones are not complete on all dataframes
available_scenario_geographies <-
  scenario_price %>%
  dplyr::distinct(scenario, scenario_geography) %>%
  dplyr::anti_join(
    scenario_price_power_not_in_capfac %>%
    dplyr::distinct(scenario, scenario_geography)
    )


readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv")) %>%
  dplyr::inner_join(available_scenario_geographies) %>%
  readr::write_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv"))
readr::read_csv(file.path(st_input_folder, "price_data_long.csv")) %>%
  dplyr::inner_join(available_scenario_geographies %>% dplyr::distinct(scenario)) %>%
  readr::write_csv(file.path(st_input_folder, "price_data_long.csv"))
readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv"))  %>%
  dplyr::inner_join(available_scenario_geographies %>% dplyr::distinct(scenario)) %>%
  readr::write_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv"))



# CLOSED SOURCE DATA

# used only in run_prepare_abcd_stress_test_input.R
# but kept in environment until end of script
# countrycode::codelist %>%
#   filter(country.name.en == "Slobakia") %>%
#   dplyr::pull(.data$ecb)
country_filter <- c()

# those 2 are deleted from the environment after the run_prepare_abcd_stress_test_input.R
filter_hqs <- FALSE
filter_assets <- FALSE

print("=================== RUNNING run_prepare_abcd_stress_test_input ===================")
source(fs::path("data-raw", "run_prepare_abcd_stress_test_input.R"))
rm(list = ls()[ls() != c("country_filter", "start_year", "st_input_folder")])

print("=================== RUNNING run_prepare_prewrangled_financial_data_stress_test ===================")
source(fs::path("data-raw", "run_prepare_prewrangled_financial_data_stress_test.R"))
rm(list = ls()[ls() != c("country_filter", "start_year", "st_input_folder")])


# ===== TRANSFORM INPUTS STRUCTURE FOR TRISK V2
st_input_folder <- here::here("data-raw", "st_inputs") # TODO USE IN ALL OTHER SCRIPTS


Scenarios_AnalysisInput <- readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv"))
abcd_stress_test_input <- readr::read_csv(file.path(st_input_folder, "abcd_stress_test_input.csv"))
prewrangled_financial_data_stress_test <- readr::read_csv(file.path(st_input_folder, "prewrangled_financial_data_stress_test.csv"))
price_data_long <- readr::read_csv(file.path(st_input_folder, "price_data_long.csv"))
prewrangled_capacity_factors <- readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv"))
bench_regions <- readr::read_rds("data-raw/bench_regions.rds")

# abcd_stress_test_input <- abcd_stress_test_input %>% filter(scenario_geography == "Global")

# ASSETS DATA

assets_data <- abcd_stress_test_input %>%
  dplyr::select(-c(scenario_geography))
# Assuming 'assets_data' is your dataframe

# Create the missing columns and initialize with the appropriate values
assets_data$country_name <- NA                          # Initialize with NA
assets_data$plant_age_years <- NA                       # Initialize with NA
assets_data$workforce_size <- NA                        # Initialize with NA

# Rename the existing columns according to the mappings

assets_data$technology <- assets_data$ald_business_unit
if (!("asset_id"  %in% names(assets_data))){
  # Step 1: Create a unique ID for each combination of company_id and technology
  unique_assets <- assets_data %>%
    distinct(company_id, technology, country_iso2) %>%
    arrange(company_id, technology, country_iso2) %>%
    mutate(asset_id = paste0(company_id, "_", row_number()))

  # Step 2: Join back to the original dataset to add the unique asset_id
  assets_data <- assets_data %>%
    left_join(unique_assets, by = c("company_id", "technology", "country_iso2"))

}
if ("scenario_geography" %in% names(assets_data)){
  assets_data <- assets_data |> 
    dplyr::filter(scenario_geography == "Global")
  assets_data$country_iso2  <- NA
}
assets_data$asset_name <- assets_data$company_name
assets_data$production_year <- assets_data$year
assets_data$emission_factor <- assets_data$plan_emission_factor
assets_data$sector <- assets_data$ald_sector
assets_data$production_unit <- assets_data$ald_production_unit

# Creates capacity factor
 assets_data <- assets_data |> 
  dplyr::inner_join(
    assets_data |>select_at(c(
        "asset_id",  
        "production_year",
        "plan_tech_prod" )) |> 
        group_by_at(c("asset_id")) |> 
        mutate(capacity_factor = plan_tech_prod/max(plan_tech_prod), capacity=max(plan_tech_prod))
        ) %>%
        ungroup() 

# capfac_data <- assets_data %>%
#   select(asset_id, production_year, capacity_factor) %>%
#   mutate(source="asset_impact")
  
# assets_data <- assets_data %>% 
#   select(-c(capacity_factor, production_year))
  

# Drop the old columns that have been replaced
assets_data <- assets_data[, !names(assets_data) %in% c("year", "ald_production_unit", "plan_emission_factor",
                                                        "ald_business_unit", "ald_sector", "plan_tech_prod",
                                                        "emissions_factor_unit" )]
# List of expected columns after renaming
expected_columns <- c(
  "asset_id", "asset_name", "company_id", "company_name", "country_iso2",
  "country_name", "technology", "sector", "plant_age_years", "workforce_size", 
  "capacity_factor" , "capacity" , "production_year", "production_unit", "emission_factor"
)

# Check if the dataframe has the expected columns
actual_columns <- names(assets_data)

stopifnot(all(expected_columns %in% names(assets_data)) && length(names(assets_data)) == length(expected_columns))
assets_data <- assets_data %>% select_at(expected_columns)
# dim_assets <- assets_data %>%
#       select_at(actual_columns) %>%
#       distinct_all()

# SCENARIOS DATA
bench_regions_agg <- bench_regions  |>
  dplyr::group_by(.data$scenario_geography_newname) |>
  dplyr::summarise(country_iso2_list = sapply(list(unique(.data$country_iso)), function(x) paste(x, collapse = ","))) %>%
   filter(scenario_geography_newname != "Global")

scenarios_data <- Scenarios_AnalysisInput %>%
  dplyr::left_join(prewrangled_capacity_factors,
                   by=c("scenario_geography", "scenario","ald_business_unit", "year")) %>%
  dplyr::inner_join(
    price_data_long %>% dplyr::select(-c(scenario_geography)) %>% distinct_all(), 
    by=c("scenario", "ald_sector", "ald_business_unit", "year") ) |>
  dplyr::left_join(
    bench_regions_agg, 
    by=c("scenario_geography"="scenario_geography_newname"))
  
scenarios_data <- scenarios_data %>%
  filter(!is.null(country_iso2_list) | !(is.null(country_iso2_list) & (scenario_geography=="Global")))

scenarios_data <- scenarios_data %>%
  mutate(capacity_factor = if_else(is.na(capacity_factor), 1, capacity_factor))
scenarios_data$scenario_capacity_factor = scenarios_data$capacity_factor
# Rename the existing columns according to the mappings
scenarios_data$sector <- scenarios_data$ald_sector
scenarios_data$technology <- scenarios_data$ald_business_unit
scenarios_data$scenario_year <- scenarios_data$year
scenarios_data$scenario_price <- scenarios_data$price
scenarios_data$price_unit <- scenarios_data$unit
scenarios_data$pathway_unit <- scenarios_data$units

scenarios_data$technology_type <- scenarios_data$direction

#   # 2. Apply capacity factors
#   hours_to_year <- 24 * 365
# scenarios_data <- scenarios_data %>% dplyr::mutate(
#   scenario_pathway = ifelse(.data$sector == "Power",
#     .data$scenario_pathway * .data$capacity_factor * .env$hours_to_year,
#     .data$scenario_pathway * .data$capacity_factor
#   )
# )

# add scenario provider column
 scenarios_data <- scenarios_data |>
    dplyr::mutate(scenario_provider = stringr::str_extract(scenario, "^[^_]+"))


scenarios_data <- scenarios_data %>% dplyr::mutate(
  technology_type = dplyr::if_else(technology_type == "declining", "carbontech", "greentech"),
  scenario_type = dplyr::if_else(scenario_type == "shock", "target", scenario_type)
)

# Create the missing columns and initialize with NA or the appropriate values
scenarios_data$price_indicator <- NA                           # Initialize with NA (missing in the provided data)

# Drop the old columns that have been replaced
scenarios_data <- scenarios_data[, !names(scenarios_data) %in% c("fair_share_perc","ald_sector", "ald_business_unit", "year", "price", "unit", "units", "capacity_factor", "direction", "indicator", "capacity_factor", "capacity_factor_unit", "price_indicator")]

# List of expected columns after renaming
expected_columns <- c(
  "scenario", "scenario_provider", "scenario_type", "scenario_geography", "sector", "technology",
  "scenario_year", "price_unit", "scenario_price", "scenario_capacity_factor",
  "pathway_unit", "scenario_pathway", "technology_type", "country_iso2_list"
)

# Check if the dataframe has the expected columns
actual_columns <- names(scenarios_data)

stopifnot(all(expected_columns %in% actual_columns) && length(actual_columns) == length(expected_columns))

scenarios_data <- scenarios_data %>% select_at(expected_columns)

# WRITE V2 DATA

st_inputs_v2_path <- fs::path("data-raw", "st_inputs_v2_ngfsv5")
fs::dir_create(st_inputs_v2_path)

scenarios_data %>% readr::write_csv(fs::path(st_inputs_v2_path, "scenarios.csv"))
assets_data |> readr::write_csv(fs::path(st_inputs_v2_path, "assets.csv"))
#financial_features.csv
prewrangled_financial_data_stress_test |>
  dplyr::select(company_id, pd, net_profit_margin, debt_equity_ratio, volatility) |>
  readr::write_csv(fs::path(st_inputs_v2_path, "financial_features.csv"))
#ngfs_carbon_price.csv
readr::read_csv(fs::path(st_input_folder, "ngfs_carbon_price.csv")) %>%
readr::write_csv(fs::path(st_inputs_v2_path, "ngfs_carbon_price.csv"))




# ********************* TRISK V1 BELOW *********************







# # ===== TEST TRISK ON ALL COMBINATIONS OF SCENARIO/GEOGRAPHY

# USE_MOCK_CLOSED_SOURCE <- TRUE

# if (USE_MOCK_CLOSED_SOURCE){
#   trisk_input_dir <- here::here("data-raw", "st_inputs_mock")

#   # Process and copy "Scenarios_AnalysisInput.csv"
#   readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv")) %>%
#     readr::write_csv(file.path(trisk_input_dir, "Scenarios_AnalysisInput.csv"))

#   # Process and copy "price_data_long.csv"
#   readr::read_csv(file.path(st_input_folder, "price_data_long.csv")) %>%
#     readr::write_csv(file.path(trisk_input_dir, "price_data_long.csv"))

#   # Process and copy "prewrangled_capacity_factors.csv"
#   readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv")) %>%
#     readr::write_csv(file.path(trisk_input_dir, "prewrangled_capacity_factors.csv"))


# # Process and copy "ngfs_carbon_price.csv"
# readr::read_csv(file.path(st_input_folder, "ngfs_carbon_price.csv")) %>%
#   readr::write_csv(file.path(trisk_input_dir, "ngfs_carbon_price.csv"))



# } else {
#   trisk_input_dir <- here::here("data-raw", "st_inputs")
# }

# scenario_geography_x_ald_sector <- trisk.model::get_scenario_geography_x_ald_sector(
#   trisk_input_dir) |>
#   dplyr::distinct(.data$baseline_scenario, .data$shock_scenario, .data$scenario_geography)

# failed_perimeters <- list()
# for (i in 1:nrow(scenario_geography_x_ald_sector)) {
#   row_params <- scenario_geography_x_ald_sector[i, ]
#   tryCatch({
#     suppressWarnings(suppressMessages(capture.output(
#       trisk.model::run_trisk(
#         input_path = trisk_input_dir,
#         output_path = tempdir(),
#         baseline_scenario = row_params$baseline_scenario,
#         shock_scenario = row_params$shock_scenario,
#         scenario_geography = row_params$scenario_geography
#       )
#     )))
#     # Uncomment if you want to print a success message
#     # cat(paste("Pass", row_params, "\n"))
#   },
#   error = function(e) {
#     message <- paste(e$message, e$parent[1]$message, sep = "; ")
#     cat(message)
#     cat(paste("Failed", row_params, "\n"))
#     # Append the error information to the failed_perimeters list in the global environment
#     failed_perimeters <<- c(failed_perimeters, list(cbind(row_params, Error = message)))
#     NULL
#   })
# }

# # Optionally, you can save the failed perimeters to a CSV file after the loop
# if (length(failed_perimeters) > 0) {
#   failed_df <- do.call(rbind, failed_perimeters)
#   readr::write_csv(failed_df, fs::path(trisk_input_dir, "failed_perimeters.csv"))
# }


# failed_df <- readr::read_csv("data-raw/st_inputs_mock/failed_perimeters.csv")

# # ===== REMOVE FAILED PERIMETERS
# app_data_dir <- file.path("data-raw", "st_inputs_app")

# # Process and copy "Scenarios_AnalysisInput.csv"
# readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv")) %>%
#     dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
#   readr::write_csv(file.path(app_data_dir, "Scenarios_AnalysisInput.csv"))

# # Process and copy "price_data_long.csv"
# readr::read_csv(file.path(st_input_folder, "price_data_long.csv")) %>%
#     dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
#   readr::write_csv(file.path(app_data_dir, "price_data_long.csv"))

# # Process and copy "prewrangled_capacity_factors.csv"
# readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv")) %>%
#     dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
#   readr::write_csv(file.path(app_data_dir, "prewrangled_capacity_factors.csv"))

# # Process and copy "ngfs_carbon_price.csv"
# readr::read_csv(file.path(st_input_folder, "ngfs_carbon_price.csv")) %>%
#     dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
#   readr::write_csv(file.path(app_data_dir, "ngfs_carbon_price.csv"))

# # ===== SAVE TO DROPBOX

# # RUN THE ABCD SAMPLING SCRIPT FIRST IF THE DATA IS UPLOADED TO THE APP (data-raw/sampling_scripts/sample_abcd_input.Rmd)

# # Save data to dropbox only if no filter applied
# if (length(country_filter) == 0) {
#   for (fp in c(
#     "abcd_stress_test_input.csv",
#     "prewrangled_financial_data_stress_test.csv",
#     "Scenarios_AnalysisInput.csv",
#     "prewrangled_capacity_factors.csv",
#     "price_data_long.csv",
#     "ngfs_carbon_price.csv"
#     )){

#     readr::write_csv(
#       readr::read_csv(here::here("data-raw", "st_inputs", fp)),
#       r2dii.utils::path_dropbox_2dii(fs::path("ST Inputs", "ST_INPUTS_MASTER", fp))
#     )

#   }
# }
