devtools::load_all()

# set overall parameters----
# based on internal research, see stress.testing.internal repo
average_npm_power <- 0.115
# start_year <- 2022 # defined in workflow.R


# prepare price data WEO 2021----

# raw data obtained from WEO2021 report, page 101
## read input data----
input_path_fossil_fuels <- fs::path(
  "data-raw",
  "price_data_long_data",
  "WEO2021_fossil_fuel_prices_by_scenario.csv"
)

input_data_fossil_fuels <- readr::read_csv(
  file.path(input_path_fossil_fuels),
  col_types = readr::cols(
    source = "c",
    sector = "c",
    unit = "c",
    scenario_geography = "c",
    scenario = "c",
    .default = readr::col_number()
  )
)

# raw data obtained from WEO2021 report, pages 333-336
input_path_power <- fs::path(
  "data-raw",
  "price_data_long_data",
  "WEO2021_power_generation_technology_costs.csv"
)

input_data_power <- readr::read_csv(
  file.path(input_path_power),
  col_types = readr::cols(
    source = "c",
    scenario = "c",
    region = "c",
    technology = "c",
    indicator = "c",
    unit = "c",
    .default = readr::col_number()
  )
)


price_data_long_WEO2021 <- prepare_price_data_long_WEO2021(
  input_data_fossil_fuel = input_data_fossil_fuels,
  input_data_power = input_data_power
)

price_data_long_adjusted_WEO2021 <- prepare_lcoe_adjusted_price_data_weo(
  input_data = price_data_long_WEO2021,
  average_npm_power = average_npm_power,
  start_year = start_year
) %>%
  dplyr::mutate(scenario = paste("WEO2021", .data$scenario, sep = "_")) %>%
  dplyr::select(-.data$source)


# prepare price data WEO 2023----

## read input data 2023----
## Fossil Fuel prices WEO2023
input_path_fossil_fuels_weo23 <- fs::path(
  "data-raw",
  "price_data_long_data",
  "raw_price_data_long_WEO2023_FF.csv"
)

input_data_fossil_fuels_weo23 <- readr::read_csv(
  file.path(input_path_fossil_fuels_weo23),
  col_types = readr::cols(
    source = "c",
    sector = "c",
    unit = "c",
    scenario_geography = "c",
    scenario = "c",
    .default = readr::col_number()
  )
)

## Power LCOE WEO2023
input_path_power_weo23 <- fs::path(
  "data-raw",
  "price_data_long_data",
  "raw_price_data_long_WEO2023_power.csv"
)

input_data_power_weo23 <- readr::read_csv(
  file.path(input_path_power_weo23),
  col_types = readr::cols(
    source = "c",
    scenario = "c",
    region = "c",
    technology = "c",
    indicator = "c",
    unit = "c",
    .default = readr::col_number()
  )
)

## price data wrangling and adjustment (uses the same function as WEO2021)
price_data_long_WEO2023 <- prepare_price_data_long_WEO2021(
  input_data_fossil_fuel = input_data_fossil_fuels_weo23,
  input_data_power = input_data_power_weo23
)

## LCOE adjustment

price_data_long_adjusted_WEO2023 <- prepare_lcoe_adjusted_price_data_weo(
  input_data = price_data_long_WEO2023,
  average_npm_power = average_npm_power,
  start_year = start_year
) %>%
  dplyr::mutate(scenario = paste("WEO2023", .data$scenario, sep = "_")) %>%
  dplyr::select(-.data$source)


# prepare price data NGFS2023----
## read input data
input_path_fossil_fuels_ngfs <- file.path("data-raw", "price_data_long_data", "raw_price_data_long_NGFSphase4.csv")

input_data_fossil_fuels_ngfs <- readr::read_csv(
  input_path_fossil_fuels_ngfs,
  col_types = readr::cols(
    Model = "c",
    Scenario = "c",
    Region = "c",
    Variable = "c",
    category_a = "c",
    category_b = "c",
    category_c = "c",
    Unit = "c",
    year = "d",
    value = "d",
    .default = readr::col_number()
  )
)

## NOTE.: NGFS uses LCOE price data Oxford2021
input_path_lcoe_oxford <- file.path("data-raw", "price_data_long_data","raw_Oxford_LCOE_wrangled.csv")

input_data_lcoe_oxford <- readr::read_delim(
  file.path(input_path_lcoe_oxford),
  col_types = readr::cols(
    Scenario = "c",
    Sector = "c",
    Region = "c",
    Technology = "c",
    Sub_Technology = "c",
    Year = "d",
    LCOE = "d",
    .default = readr::col_number()
  )
)


price_data_long_NGFS2023 <- prepare_price_data_long_NGFS2023(
  input_data_fossil_fuels_ngfs = input_data_fossil_fuels_ngfs,
  start_year = start_year
)

lcoe_adjusted_price_data_oxford2021_2022 <- prepare_lcoe_adjusted_price_data_oxford2022(
  input_data_lcoe_oxford = input_data_lcoe_oxford,
  average_npm_power = average_npm_power,
  start_year = start_year,
  ngfs_vintage = 2023
)

price_data_long_adjusted_NGFS2023 <- price_data_long_NGFS2023 %>%
  dplyr::bind_rows(lcoe_adjusted_price_data_oxford2021_2022)


# prepare price data NGFS2024 - Phase 5----
## read input data

input_path_fossil_fuels_ngfsv5 <- file.path("data-raw", "price_data_long_data", "raw_price_data_long_NGFSphase5.csv")

input_data_fossil_fuels_ngfsv5 <- readr::read_csv(
  input_path_fossil_fuels_ngfsv5,
  col_types = readr::cols(
    Model = "c",
    Scenario = "c",
    Region = "c",
    Variable = "c",
    category_a = "c",
    category_b = "c",
    category_c = "c",
    Unit = "c",
    year = "d",
    value = "d",
    .default = readr::col_number()
  )
)

## NOTE.: NGFS uses LCOE price data Oxford2021
input_path_lcoe_oxford <- file.path("data-raw", "price_data_long_data","raw_Oxford_LCOE_wrangled.csv")

input_data_lcoe_oxford <- readr::read_delim(
  file.path(input_path_lcoe_oxford),
  col_types = readr::cols(
    Scenario = "c",
    Sector = "c",
    Region = "c",
    Technology = "c",
    Sub_Technology = "c",
    Year = "d",
    LCOE = "d",
    .default = readr::col_number()
  )
)

price_data_long_NGFS2024 <- prepare_price_data_long_NGFS2024(
  input_data_fossil_fuels_ngfsv5 = input_data_fossil_fuels_ngfsv5,
  start_year = start_year
)

lcoe_adjusted_price_data_oxford2021_2022_ngfs2024 <- prepare_lcoe_adjusted_price_data_oxford2022(
  input_data_lcoe_oxford = input_data_lcoe_oxford,
  average_npm_power = average_npm_power,
  start_year = start_year,
  ngfs_vintage = 2024
)

price_data_long_adjusted_NGFS2024 <- price_data_long_NGFS2024 %>%
  dplyr::bind_rows(lcoe_adjusted_price_data_oxford2021_2022_ngfs2024)






### prepare price data IPR 2023
## read input data----
input_path_fossil_fuels_ipr <- file.path("data-raw", "price_data_long_data","raw_price_data_long_IPR2023.csv")

input_data_fossil_fuels_ipr <- readr::read_delim(
  file.path(input_path_fossil_fuels_ipr),
  col_types = readr::cols(
    Scenario = "c",
    Region = "c",
    Variable_class = "c",
    Sub_variable_class_1 = "c",
    Units = "c",
    year = "d",
    value = "d",
    .default = readr::col_number()
  )
)

price_data_long_IPR2023 <- prepare_price_data_long_IPR2023(input_data_fossil_fuels_ipr,
  start_year = start_year
)

## NOTE: IPR prices for the power sector uses LCOE data from WEO2021 (input_data_power, see above)

price_data_power_IPR2023 <- prepare_price_data_long_Power_IPR2023(input_data_power)

lcoe_adjusted_price_data_IPR2023 <- prepare_lcoe_adjusted_price_data_IPR2023(
  input_data = price_data_power_IPR2023,
  average_npm_power = average_npm_power,
  start_year = start_year
) %>%
  dplyr::select(-.data$source)

## IPR baseline

price_data_IPR2023_baseline <- prepare_price_data_long_IPR2023_baseline(price_data_long_adjusted_WEO2021)

### Total combined IPR2021 price data

price_data_long_adjusted_IPR2023 <- price_data_long_IPR2023 %>%
  dplyr::bind_rows(lcoe_adjusted_price_data_IPR2023) %>%
  dplyr::bind_rows(price_data_IPR2023_baseline)


## prepare price data Oxford
## read input data

input_path_fossil_fuels_oxf <- file.path("data-raw", "price_data_long_data","raw_price_data_long_OXF2021.csv")

input_data_fossil_fuels_oxf <- readr::read_delim(
  file.path(input_path_fossil_fuels_oxf),
  col_types = readr::cols(
    Scenario = "c",
    Sector = "c",
    Region = "c",
    Technology = "c",
    Year = "d",
    LCOE = "d",
    .default = readr::col_number()
  )
)

price_data_long_adjusted_OXF2021 <- prepare_price_data_long_Oxf2021(input_data_fossil_fuels_oxf,
  start_year = start_year
)

### NOTE: Oxford power prices are already in the data through lcoe_adjusted_price_data_oxford2021_2022

## Steel Price Data
## For Steel prices, we transform LC values given by MP into prices.
input_path_steel_LC <- file.path("data-raw", "price_data_long_data","raw_MP_LC_steel.csv")

steel_lc_raw <- readr::read_delim(
  input_path_steel_LC,
  delim = ";",
  col_types = readr::cols(
    scenario = "c",
    year = "d",
    region = "c",
    technology = "c",
    levelized_cost = "d",
    .default = readr::col_number()
  )
)

# processig raw Steel LC data
steel_lc_data <- MP_LC_steel_wrangling(steel_lc_raw)

#creating steel price data
#average steel npm (to be updated)
steel_npm <- 0.06866203

price_data_long_adjusted_MP_Steel <- prepare_lc_adjusted_price_data_steel(input_data=steel_lc_data, average_npm_steel = steel_npm, start_year = start_year)


## prepare price data Automotive

# scenarios with automotive sector are identified in the scenario file
Scenarios_AnalysisInput <- readr::read_csv(fs::path(
  "data-raw","st_inputs","Scenarios_AnalysisInput.csv"
))

auto_prices <- create_automotive_prices(Scenarios_AnalysisInput)

## combine and write all price data----

price_data_long_adjusted <- price_data_long_adjusted_WEO2021 %>%
  dplyr::bind_rows(price_data_long_adjusted_WEO2023) %>%
  dplyr::bind_rows(price_data_long_adjusted_NGFS2023) %>%
  dplyr::bind_rows(price_data_long_adjusted_NGFS2024) %>%
  dplyr::bind_rows(price_data_long_adjusted_IPR2023) %>%
  dplyr::bind_rows(price_data_long_adjusted_OXF2021) %>%
  dplyr::bind_rows(price_data_long_adjusted_MP_Steel) %>%
  dplyr::bind_rows(auto_prices)



price_data_long_adjusted %>%
  dplyr::rename(ald_business_unit = .data$technology,
                ald_sector = .data$sector) %>%
  # doing hardcoded filtering directly upon import as we currently do not
  # differentiate scenario_geographies for price data
  dplyr::filter(scenario_geography == "Global") %>%
  readr::write_csv(file.path("data-raw", "st_inputs", "price_data_long.csv"))
