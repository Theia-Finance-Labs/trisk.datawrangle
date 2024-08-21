devtools::load_all()


#vector of low carbon technologies
green_techs <- c(
  "FuelCell",
  "Electric",
  "Hybrid",
  "RenewablesCap",
  "HydroCap",
  "NuclearCap",
  "FuelCell_HDV",
  "Electric_HDV",
  "Hybrid_HDV"
)

# scenario values will be linearly interpolated for each group below
interpolation_groups <- c(
  "source",
  "scenario",
  "sector",
  "technology",
  "scenario_geography",
  "indicator",
  "units"
)


#WEO data from PACTA routine
 input_path <- fs::path(
   "data-raw",
   "scenario_analysis_input_data",
   "weo_Scenarios_AnalysisInput.csv"
 )

 weo_data <- readr::read_csv(
   input_path,
   col_types = readr::cols_only(
     source = "c",
     scenario = "c",
     scenario_geography = "c",
     sector = "c",
     technology = "c",
     units = "c",
     indicator = "c",
     year = "d",
     value = "d"
   )
 )

# We use GECO2021 data for IPR2023 Automotive baseline and GECO2023 data for our 
# usual GECO Automotive Scenarios. 
# Todo: decouple GECO2021 from WEO routine
#GECO2021 data from PACTA routine
 input_path <- fs::path(
      "data-raw",
   "scenario_analysis_input_data",
   "pacta_processed_geco_Scenarios_AnalysisInput.csv"
 )

 geco2021_data <- readr::read_csv(
   input_path,
   col_types = readr::cols_only(
     source = "c",
     scenario = "c",
     scenario_geography = "c",
     sector = "c",
     technology = "c",
     units = "c",
     indicator = "c",
     year = "d",
     value = "d"
   )
 )

# combine WEO with GECO data
 weo_geco_data <- rbind(
   weo_data,
   geco2021_data
 )

 weo_geco_data <- weo_geco_data %>%
   interpolate_yearly(!!!rlang::syms(interpolation_groups)) %>%
   dplyr::filter(year >= start_year) %>%
   add_market_share_columns(start_year = start_year)

 weo_geco_data <- weo_geco_data %>%
   format_p4i(green_techs)

prepared_data <- prepare_scenario_data(data = weo_geco_data)


# WEO 2023 Global
# Only Global for now, due to data availability issues

#Preprocessed WEO data
input_path_weo23 <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "weo23_Scenarios_AnalysisInput.csv"
)

weo23_data <- readr::read_csv(
  input_path_weo23,
  col_types = readr::cols_only(
    source = "c",
    scenario = "c",
    scenario_geography = "c",
    sector = "c",
    technology = "c",
    units = "c",
    indicator = "c",
    year = "d",
    value = "d"
  )
)


weo23_data  <- weo23_data %>%
  interpolate_yearly(!!!rlang::syms(interpolation_groups)) %>%
  dplyr::filter(year >= start_year) %>%
  add_market_share_columns(start_year = start_year)

weo23_data <- weo23_data %>%
  format_p4i(green_techs)

prepared_data_weo23 <- prepare_scenario_data_weo23(data = weo23_data)

## GECO2023 (only Automotive)

input_path <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "GECO2023_AnalysisInput.csv"
)

geco2023_data <- readr::read_csv(
  input_path,
  col_types = readr::cols_only(
    source = "c",
    scenario = "c",
    scenario_geography = "c",
    sector = "c",
    technology = "c",
    units = "c",
    indicator = "c",
    year = "d",
    value = "d"
  )
)

# interpolating and tsmr and smsr calculations

geco2023_data <- geco2023_data %>%
  interpolate_yearly(!!!rlang::syms(interpolation_groups)) %>%
  dplyr::filter(.data$year >= start_year) %>%
  add_market_share_columns(start_year = start_year)

geco2023_data <- geco2023_data %>%
  format_p4i(green_techs)

prepared_geco23_data <- prepare_geco2023(data=geco2023_data)

#NGFS Phase IV
input_path <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "ngfs_Scenarios_AnalysisInput_phase4.csv"
)

ngfs_data <- readr::read_csv(
  input_path,
  col_types = readr::cols_only(
    Model = "c",
    Scenario = "c",
    Region = "c",
    Variable = "c",
    category_a = "c",
    category_b = "c",
    category_c = "c",
    Unit = "c",
    year = "d",
    value = "d"
  )
) %>%
  dplyr::mutate(Scenario = gsub("°" , " ", .data$Scenario))

preprepared_ngfs_data <- preprepare_ngfs_scenario_data(ngfs_data,
                                                       start_year= start_year)


preprepared_ngfs_data <- preprepared_ngfs_data %>%
  interpolate_yearly(!!!rlang::syms(interpolation_groups)) %>%
  dplyr::filter(year >= start_year) %>%
  add_market_share_columns(start_year = start_year)

preprepared_ngfs_data <- preprepared_ngfs_data %>% format_p4i(green_techs)

preprepared_ngfs_data <- style_ngfs(preprepared_ngfs_data)

# replace nan fair_share_perc by 0. Nans appear when dividing per 0 in the tmsr computation
preprepared_ngfs_data <- preprepared_ngfs_data %>%
  dplyr::mutate(fair_share_perc = dplyr::if_else(is.na(fair_share_perc), 0, fair_share_perc))


### IPR Scenario 2023
### Read IPR

input_path <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "ipr2023_Scenarios_AnalysisInput.csv"
)

IPR <- as.data.frame(readr::read_csv(
  input_path,
  col_types = readr::cols_only(
    Scenario = "c",
    Region = "c",
    Sector = "c",
    Units = "c",
    Variable_class = "c",
    Sub_variable_class_1 = "c",
    Sub_variable_class_2 = "c",
    year = "d",
    value = "d"
  )
))

prepared_IPR_data <- prepare_IPR_scenario_data2023(IPR,
                                               start_year = start_year)

# IPR baseline scenario
# IPR baseline is a duplicate of the WEO2021 STEPs scenario for Coal, OG and Power
# IPR baseline is a duplicate of the JRC Geco scenario for Automotive

IPR_baseline <- prepare_IPR_baseline_scenario(prepared_data)

# IPR Automotive

ipr_automotive_baseline_data <- geco2021_data
ipr_automotive_baseline_data <- ipr_automotive_baseline_data %>%
  interpolate_yearly(!!!rlang::syms(interpolation_groups)) %>%
  dplyr::filter(year >= start_year) %>%
  add_market_share_columns(start_year = start_year)

# Different green tech categorization for the IPR baseline, based on IPR FPS 
green_techs_ipr <- c("RenewablesCap", "HydroCap", "NuclearCap", "SolarCap", "OffWindCap", "OnWindCap", "BiomassCap",
                     "Electric", "FuelCell")

ipr_automotive_baseline_data <- ipr_automotive_baseline_data %>%
  format_p4i(green_techs_ipr)

IPR_baseline_automotive <- prepare_IPR_baseline_scenario_automotive(ipr_automotive_baseline_data)

#joining IPR scenarios

prepared_IPR_data <- dplyr::full_join(prepared_IPR_data, IPR_baseline)
prepared_IPR_data <- dplyr::full_join(prepared_IPR_data, IPR_baseline_automotive)


# replace nan fair_share_perc by 0. Nans appear when dividing per 0 in the tmsr computation
prepared_IPR_data <- prepared_IPR_data %>%
  dplyr::mutate(fair_share_perc = dplyr::if_else(is.na(fair_share_perc), 0, fair_share_perc))

### Oxford Scenario
### Read Oxford

input_path <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "oxford_Scenarios_AnalysisInput.csv"
)

OXF <- as.data.frame(readr::read_csv(
  input_path,
  col_types = readr::cols_only(
    "Annual energy" = "c",
    units = "c",
    scenario = "c",
    scenario_geography = "c",
    year = "d",
    value = "d"
  )
))
prepared_OXF_data <- prepare_OXF_scenario_data(OXF,
                                               start_year = start_year)


## Mission Possible Steel Scenarios

input_path_steel <- fs::path(
  "data-raw",
  "scenario_analysis_input_data",
  "MP_steel_Scenario_Analysis_Input.csv"
)

STEEL <- as.data.frame(readr::read_csv(
  input_path_steel,
  col_types = readr::cols_only(
    scenario = "c",
    technology = "c",
    year = "d",
    "Production (Mt)" = "d"
  )
))

prepared_steel_data <- prepare_steel_scenario_data(STEEL, start_year = start_year)


### Merge Data from Scenario Sources
prepared_data_IEA_NGFS <- dplyr::full_join(prepared_data, preprepared_ngfs_data)
prepared_data_IPR_OXF <- dplyr::full_join(prepared_IPR_data, prepared_OXF_data)
prepared_data_combined <- dplyr::full_join(prepared_data_IEA_NGFS, prepared_data_IPR_OXF)
prepared_data_combined <- dplyr::full_join(prepared_data_combined, prepared_geco23_data)
prepared_data_combined <- dplyr::full_join(prepared_data_combined, prepared_steel_data)
prepared_data_combined <- dplyr::full_join(prepared_data_combined, prepared_data_weo23)


baseline_scenarios <- c(
  "WEO2021_STEPS",
  "WEO2023_STEPS",
  "GECO2021_CurPol",
  "GECO2023_CurPol",
  "WEO2021_APS",
  "NGFS2023GCAM_CP",
  "NGFS2023MESSAGE_CP",
  "NGFS2023REMIND_CP",
  "NGFS2023MESSAGE_FW",
  "NGFS2023REMIND_FW",
  "NGFS2023GCAM_FW",
  "NGFS2023MESSAGE_NDC",
  "NGFS2023REMIND_NDC",
  "NGFS2023GCAM_NDC",
  "IPR2023_baseline",
  "IPR2023Automotive_baseline",
  "Oxford2021_base",
  "Steel_baseline"
)
shock_scenarios <- c(
    "WEO2021_SDS",
    "WEO2021_NZE_2050",
    "WEO2023_APS",
    "WEO2023_NZE_2050",
    "GECO2021_1.5C-Unif",
    "GECO2021_NDC-LTS",
    "GECO2023_1.5C",
    "GECO2023_NDC-LTS",
    "NGFS2023GCAM_B2DS",
    "NGFS2023MESSAGE_B2DS",
    "NGFS2023REMIND_B2DS",
    "NGFS2023GCAM_LD",
    "NGFS2023MESSAGE_LD",
    "NGFS2023REMIND_LD",
    "NGFS2023GCAM_DT",
    "NGFS2023MESSAGE_DT",
    "NGFS2023REMIND_DT",
    "NGFS2023GCAM_NZ2050",
    "NGFS2023MESSAGE_NZ2050",
    "NGFS2023REMIND_NZ2050",
    "IPR2023_FPS",
    "IPR2023Automotive_FPS",
    "Oxford2021_fast",
    "Steel_NZ"
)

prepared_data_combined <- prepared_data_combined %>%
  dplyr::mutate(
    scenario_type= dplyr::case_when(
      .data$scenario %in% baseline_scenarios ~ "baseline",
      .data$scenario %in% shock_scenarios ~ "shock",
      TRUE ~ NA_character_  # Assign NA for scenarios not in either list
    )
  ) %>%
  assertr::verify(sum(is.na(scenario_type)) == 0)

prepared_data_combined %>%
  dplyr::rename(ald_business_unit=.data$technology) %>%
  readr::write_csv(
  file.path("data-raw", "st_inputs", "Scenarios_AnalysisInput.csv")
)
