options(r2dii_dropbox=r2dii_dropbox)

start_year <- 2022

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

capacity_factors_power <- trisk.model:::read_capacity_factors_power(
  trisk.model:::capacity_factor_file(st_input_folder))
df_price <- trisk.model:::read_price_data(
  trisk.model:::price_data_file(st_input_folder))
scenario_data = trisk.model:::read_scenario_data(
  trisk.model:::scenario_data_file(st_input_folder))

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

# ===== TEST TRISK ON ALL COMBINATIONS OF SCENARIO/GEOGRAPHY

USE_MOCK_CLOSED_SOURCE <- TRUE

if (USE_MOCK_CLOSED_SOURCE){
  trisk_input_dir <- here::here("data-raw", "st_inputs_mock")

  # Process and copy "Scenarios_AnalysisInput.csv"
  readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv")) %>%
    readr::write_csv(file.path(trisk_input_dir, "Scenarios_AnalysisInput.csv"))

  # Process and copy "price_data_long.csv"
  readr::read_csv(file.path(st_input_folder, "price_data_long.csv")) %>%
    readr::write_csv(file.path(trisk_input_dir, "price_data_long.csv"))

  # Process and copy "prewrangled_capacity_factors.csv"
  readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv")) %>%
    readr::write_csv(file.path(trisk_input_dir, "prewrangled_capacity_factors.csv"))


# Process and copy "ngfs_carbon_price.csv"
readr::read_csv(file.path(st_input_folder, "ngfs_carbon_price.csv")) %>%
  readr::write_csv(file.path(trisk_input_dir, "ngfs_carbon_price.csv"))

    

} else {
  trisk_input_dir <- here::here("data-raw", "st_inputs")
}

scenario_geography_x_ald_sector <- trisk.model::get_scenario_geography_x_ald_sector(
  trisk_input_dir) |>
  dplyr::distinct(.data$baseline_scenario, .data$shock_scenario, .data$scenario_geography)

failed_perimeters <- list()
for (i in 1:nrow(scenario_geography_x_ald_sector)) {
  row_params <- scenario_geography_x_ald_sector[i, ]
  tryCatch({
    suppressWarnings(suppressMessages(capture.output(
      trisk.model::run_trisk(
        input_path = trisk_input_dir,
        output_path = tempdir(),
        baseline_scenario = row_params$baseline_scenario,
        shock_scenario = row_params$shock_scenario,
        scenario_geography = row_params$scenario_geography
      )
    )))
    # Uncomment if you want to print a success message
    # cat(paste("Pass", row_params, "\n"))
  },
  error = function(e) {
    message <- paste(e$message, e$parent[1]$message, sep = "; ")
    cat(message)
    cat(paste("Failed", row_params, "\n"))
    # Append the error information to the failed_perimeters list in the global environment
    failed_perimeters <<- c(failed_perimeters, list(cbind(row_params, Error = message)))
    NULL
  })
}

# Optionally, you can save the failed perimeters to a CSV file after the loop
if (length(failed_perimeters) > 0) {
  failed_df <- do.call(rbind, failed_perimeters)
  readr::write_csv(failed_df, fs::path(trisk_input_dir, "failed_perimeters.csv"))
}


failed_df <- readr::read_csv("data-raw/st_inputs_mock/failed_perimeters.csv")

# ===== REMOVE FAILED PERIMETERS
app_data_dir <- file.path("data-raw", "st_inputs_app")

# Process and copy "Scenarios_AnalysisInput.csv"
readr::read_csv(file.path(st_input_folder, "Scenarios_AnalysisInput.csv")) %>%
    dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
  readr::write_csv(file.path(app_data_dir, "Scenarios_AnalysisInput.csv"))

# Process and copy "price_data_long.csv"
readr::read_csv(file.path(st_input_folder, "price_data_long.csv")) %>%
    dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
  readr::write_csv(file.path(app_data_dir, "price_data_long.csv"))

# Process and copy "prewrangled_capacity_factors.csv"
readr::read_csv(file.path(st_input_folder, "prewrangled_capacity_factors.csv")) %>%
    dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
  readr::write_csv(file.path(app_data_dir, "prewrangled_capacity_factors.csv"))

# Process and copy "ngfs_carbon_price.csv"
readr::read_csv(file.path(st_input_folder, "ngfs_carbon_price.csv")) %>%
    dplyr::anti_join(failed_df %>% dplyr::select(baseline_scenario, shock_scenario, scenario_geography)) %>%
  readr::write_csv(file.path(app_data_dir, "ngfs_carbon_price.csv"))

# ===== SAVE TO DROPBOX

# RUN THE ABCD SAMPLING SCRIPT FIRST IF THE DATA IS UPLOADED TO THE APP (data-raw/sampling_scripts/sample_abcd_input.Rmd) 

# Save data to dropbox only if no filter applied
if (length(country_filter) == 0) {
  for (fp in c(
    "abcd_stress_test_input.csv",
    "prewrangled_financial_data_stress_test.csv",
    "Scenarios_AnalysisInput.csv",
    "prewrangled_capacity_factors.csv",
    "price_data_long.csv",
    "ngfs_carbon_price.csv"
    )){
    
    readr::write_csv(
      readr::read_csv(here::here("data-raw", "st_inputs", fp)),
      r2dii.utils::path_dropbox_2dii(fs::path("ST Inputs", "ST_INPUTS_MASTER", fp))
    )
    
  }
} 