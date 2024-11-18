devtools::load_all()

##set start year
# start_year <- 2022 # defined in workflow.R

# prepare capacity factor data WEO 2021

# NOTE: the WEO2021 raw data did not deliver the required data points to
# sufficiently cover all scenario geographies. We therefor use the capacity
# factors from WEO2020 again
# For consistency in the code, we relabel the source to WEO2021 and we use
# capacity factors from SDS to fill in for the NZE scenario. We also use STEPS
# capacity factors to fill in for the APS scenario as - in both cases - these
# seem to be the closest in narrative.

## read data
input_path <- fs::path(
  "data-raw",
  "capacity_factors_data",
  "WEO2020_Raw_Data.csv"
)

data <- readr::read_csv(
  input_path,
  col_types = readr::cols(
    Source = "c",
    Indicator = "c",
    Sector = "c",
    Units = "c",
    Scenario = "c",
    ScenarioGeography = "c",
    Technology = "c",
    Sub_Technology = "c",
    SourceSheet = "c",
    .default = readr::col_number()
  )
)

## prepare data
prepared_data_WEO2021 <- prepare_prewrangled_capacity_factors_WEO2021(data, start_year = start_year)

## WEO2023

## read data
input_path_weo23 <- fs::path(
  "data-raw",
  "capacity_factors_data",
  "raw_capacity_factors_WEO2023.csv"
)

data_weo23 <- readr::read_csv(
  input_path_weo23,
  col_types = readr::cols(
    source = "c",
    scenario = "c",
    scenario_geography = "c",
    sector = "c",
    indicator = "c",
    technology = "c",
    units = "c",
    year = "d",
    value = "d",
    .default = readr::col_number()
  )
)

## prepare data
prepared_data_WEO2023 <- prepare_prewrangled_capacity_factors_WEO2023(data_weo23, start_year = start_year)


##NGFS Phase 4
input_path <- file.path("data-raw", "capacity_factors_data", "raw_capacity_factors_NGFSphase4.csv")

data <- readr::read_csv(
  input_path,
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

## prepare data
prepared_data_NGFS2023 <- prepare_capacity_factors_NGFS2023(data, start_year = start_year)


##NGFS Phase 5
input_path <- file.path("data-raw", "capacity_factors_data", "raw_capacity_factors_NGFSphase5.csv")

data <- readr::read_csv(
  input_path,
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

## prepare data
prepared_data_NGFS2024 <- prepare_capacity_factors_NGFS2024(data, start_year = start_year)

### IPR2023 data

input_path <- fs::path("data-raw", "capacity_factors_data","raw_capacity_factors_IPR2023.csv")

data <- readr::read_csv(
  input_path,
  col_types = readr::cols(
    Variable_class = "c",
    Sub_variable_class_1 = "c",
    Sub_variable_class_2 = "c",
    Scenario = "c",
    Region = "c",
    Sector = "c",
    Units = "c",
    year = "d",
    value = "d",
    .default = readr::col_number()
  )
)

## prepare IPR data
prepared_data_IPR2023 <- prepare_capacity_factors_IPR2023(data, start_year = start_year) %>%
  dplyr::filter(.data$scenario_geography != "IND") # delete dulicated india geography

## IPR baseline CF is a duplicate of IPR2023_FPS
IPR_baseline <- prepare_capacity_factors_IPR2023_baseline(prepared_data_IPR2023)

# merging IPR CF data
prepared_data_IPR2023 <- dplyr::full_join(prepared_data_IPR2023, IPR_baseline)

### Oxford data
# Oxford uses Capacity Factors from WEO2021

prepared_data_OXF2021 <- prepare_capacity_factors_OXF2021(prepared_data_WEO2021)

## Steel Capacity Factors - GEM
input_path_steel <- file.path("data-raw", "capacity_factors_data", "preprocessed_capacity_factors_GEM_Steel.csv")

# Steel CF based on the GEM database for 2021 capacity and production of steel
steel_cf <- readr::read_csv(
  input_path_steel,
  col_types = readr::cols(
    year = "d",
    technology = "c",
    value = "d",
    .default = readr::col_number()
  )
)

prepared_data_steel <- prepare_capacity_factors_GEM_steel(steel_cf, start_year=start_year)

## combine and write data
prepared_data <- prepared_data_WEO2021 %>%
  dplyr::bind_rows(prepared_data_NGFS2023) %>%
  dplyr::bind_rows(prepared_data_NGFS2024) %>%
  dplyr::bind_rows(prepared_data_IPR2023) %>%
  dplyr::bind_rows(prepared_data_OXF2021) %>%
  dplyr::bind_rows(prepared_data_steel) %>%
  dplyr::bind_rows(prepared_data_WEO2023)



prepared_data %>%
  dplyr::rename(ald_business_unit=.data$technology) %>%
  readr::write_csv(
  file.path("data-raw", "st_inputs","prewrangled_capacity_factors.csv")
)
