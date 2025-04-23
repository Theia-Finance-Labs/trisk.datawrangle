#' Interpolate values in a dataset, by year.
#'
#' @param data An input dataset. Must contain the columns `year` and `value`.
#' @param ... Other grouping variables. `value` will be interpolated for each
#'   group.
#'
#' @return A dataset with the column `value` interpolated linearly against the
#'   column `year`.
#'
#' @export
interpolate_yearly <- function(data, ...) {
  data %>%
    dplyr::group_by(...) %>%
    tidyr::complete(year = tidyr::full_seq(.data$year, 1)) %>%
    dplyr::mutate(
      value = zoo::na.approx(.data$value, .data$year, na.rm = FALSE)
    ) %>%
    dplyr::ungroup()

}

add_technology_fair_share_ratio <- function(data) {
  data %>%
    dplyr::ungroup() %>%
    dplyr::group_by(!!!rlang::syms(c(common_fs_groups(), "technology"))) %>%
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    dplyr::mutate(tmsr = (.data$value - dplyr::first(.data$value)) / dplyr::first(.data$value)) %>%
    dplyr::ungroup()
}

add_market_fair_share_percentage <- function(data) {
  data %>%
    dplyr::ungroup() %>%
    dplyr::group_by(!!!rlang::syms(c(common_fs_groups(), "year"))) %>%
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    dplyr::mutate(sector_total_by_year = sum(.data$value)) %>%
    dplyr::group_by(!!!rlang::syms(c(common_fs_groups(), "technology"))) %>%
    dplyr::mutate(
      smsp = (.data$value - dplyr::first(.data$value)) /
        dplyr::first(.data$sector_total_by_year),
      sector_total_by_year = NULL
    ) %>%
    dplyr::ungroup()
}

common_fs_groups <- function() {
  c("scenario", "sector", "scenario_geography")
}

#' Add market share columns to a scenario dataset
#'
#' Calculates and adds market share values (ie. technology market-share ratio
#' and sector market-share percentage) to a scenario dataset. A reference
#' start-year must be provided.
#'
#' @param data A scenario dataset.
#' @param start_year The baseline year, against which the technology- and
#'   sector- market shares will be calculated. Note: At the start year, tmsr = 1
#'   and smsp =0 respectively.
#'
#' @return A scenario dataset, with the new columns `tmsr` and `smsp`.
#'
#' @export
add_market_share_columns <- function(data, start_year) {
  old_groups <- dplyr::groups(data)
  data <- dplyr::ungroup(data)

  data %>%
    add_technology_fair_share_ratio() %>%
    add_market_fair_share_percentage() %>%
    dplyr::group_by(!!!old_groups)
}


#' Check if a named object contains expected names
#'
#' Based on fgeo.tool::check_crucial_names()
#'
#' @param x A named object.
#' @param expected_names String; expected names of `x`.
#'
#' @return Invisible `x`, or an error with informative message.
#'
#' @examples
#' x <- c(a = 1)
#' check_crucial_names(x, "a")
#' try(check_crucial_names(x, "bad"))
#' @noRd
check_crucial_names <- function(x, expected_names) {
  stopifnot(rlang::is_named(x))
  stopifnot(is.character(expected_names))

  ok <- all(unique(expected_names) %in% names(x))
  if (!ok) {
    abort_missing_names(sort(setdiff(expected_names, names(x))))
  }

  invisible(x)
}

abort_missing_names <- function(missing_names) {
  nms <- glue::glue_collapse(missing_names, sep = ", ", last = ", and ")
  abort(glue::glue("Must have missing names:\n{nms}."), class = "missing_names")
}

abort_missing_names <- function(missing_names) {
  nms <- glue::glue_collapse(missing_names, sep = ", ", last = ", and ")
  abort(glue::glue("Must have missing names:\n{nms}."), class = "missing_names")
}


#' Format scenario data for P4I
#'
#' @param data A scenario dataset.
#' @param green_techs A list of green technologies. For these, a `direction` of
#'   "increasing" will be assigned, and the `smsp` column will be used to assign
#'   a `FairSharePerc`. Otherwise the `direction` will be `decreasing` and the
#'   `tmsr` column will be used.
#'
#' @return A scenario dataset, with columns renamed to be consistent with
#'   pacta.data.preparation input requirements.
#' @export
format_p4i <- function(data, green_techs) {

    crucial_names <- c(
    "source",
    "scenario",
    "scenario_geography",
    "sector",
    "technology",
    "indicator",
    "units",
    "year",
    "tmsr",
    "smsp"
  )

  check_crucial_names(data, crucial_names)

  data %>%
   dplyr::mutate(Sub_Technology = NA) %>% # this column should be dropped from PACTA
   dplyr::mutate(
    Direction = dplyr::if_else(.data$technology %in% .env$green_techs, "increasing", "declining"),
    FairSharePerc = dplyr::if_else(.data$Direction == "declining", .data$tmsr, .data$smsp)) %>%
    dplyr::select(
      Source = .data$source,
      ScenarioGeography = .data$scenario_geography,
      Scenario = .data$scenario,
      Sector = .data$sector,
      Technology = .data$technology,
      .data$Sub_Technology,
      Indicator = .data$indicator,
      Units = .data$units,
      Year = .data$year,
      scenario_pathway=.data$value,
      techFSRatio = .data$tmsr,
      mktFSRatio = .data$smsp,
      .data$Direction,
      .data$FairSharePerc
    )


}

#' This function reads scenario data in the form of data as found in the dropbox
#' under "Processed Data" and wrangles it to fit the required format for the
#' stress test.
#'
#' @param data Tibble that contains the scenario data file that is to be
#'   processed
#' @family data preparation functions
#' @export
prepare_scenario_data <- function(data) {
  data_has_expected_columns <- all(
    c(
      "Source", "Technology", "ScenarioGeography", "Sector", "Units",
      "Indicator", "Scenario", "Sub_Technology", "Year", "Direction", "mktFSRatio", "techFSRatio",
      "FairSharePerc"
    ) %in% colnames(data)
  )

  stopifnot(data_has_expected_columns)

  # due to inconsistencies in the raw data across sources, we need to filter for
  # other Indicators in IEA scenarios than in GECO scenarios at least up until
  # WEO 2021 and GECO 2021. Please review once new scenarios are available
  data <- data %>%
    dplyr::filter(
      (stringr::str_detect(.data$Source, "WEO2021") & .data$Indicator %in% c("Capacity", "Energy Supply", "Production", "Sales")) |
        (stringr::str_detect(.data$Source, "GECO2021") & .data$Indicator %in% c("Capacity", "Production", "Sales"))
    ) %>%
    # for GECO2021, we can only cover Automotive, as we do not have any price or
    # cost information for other sectors
    dplyr::filter(
      !(stringr::str_detect(.data$Source, "GECO2021") & .data$Sector != "Automotive")
    ) %>%
    dplyr::filter(
      !(.data$Technology == "RenewablesCap" & !is.na(.data$Sub_Technology)) # THIS NEEDS TO BE INVESTIGATED AS SUBTECHNOLOGIES CURRENTLY ALWAYS EMPTY
    ) %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio
      )
    ) %>%
    dplyr::rename(
      scenario_source = .data$Source,
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    ) %>%
    dplyr::relocate(
      .data$scenario_source, .data$scenario_geography, .data$scenario,
      .data$ald_sector, .data$units, .data$technology, .data$year,
      .data$direction, .data$fair_share_perc
    ) %>%
    dplyr::mutate(
      scenario = stringr::str_c(.data$scenario_source, .data$scenario, sep = "_")
    ) %>%
    dplyr::distinct_all()

  # We can only use scenario x scenario_geography combinations that do not have
  # NAs on any not nullable columns. We currently  use STEPS, SDS, APS and NZE_2050, thus for
  # now only affected scenario x scenario_geography combinations in those sectors
  # are removed. Rows are removed from all scenarios as soon as scenario_geography
  # is affected on any of the scenarios.
  NA_geos <- data %>%
    dplyr::filter(
      .data$scenario %in% c(
        "WEO2021_STEPS", "WEO2021_SDS", "WEO2021_NZE_2050", "WEO2021_APS",
        "WEO2020_SPS", "GECO2021_CurPol", "GECO2021_1.5C-Unif", "GECO2021_NDC-LTS"
      )
    ) %>%
    dplyr::filter_all(dplyr::any_vars(is.na(.))) %>%
    dplyr::distinct(.data$scenario_source, .data$scenario_geography, .data$ald_sector)

  data <- data %>%
    dplyr::anti_join(NA_geos, by = c("scenario_source", "scenario_geography", "ald_sector"))

  # removing sectors that are not supported by stress testing
  p4i_p4b_sector_technology_lookup_df <- p4i_p4b_sector_technology_lookup()

  data <- data %>%
    dplyr::filter(.data$ald_sector %in% unique(p4i_p4b_sector_technology_lookup_df$sector_p4i))

  data <- remove_incomplete_sectors(data)

  data <- data %>%
    dplyr::select(-.data$scenario_source)

  return(data)
}

#' WEO2023 preparation
#' 2023 vintage has only global data
#'
#' @param data data
prepare_scenario_data_weo23 <- function(data) {
  data_has_expected_columns <- all(
    c(
      "Source", "Technology", "ScenarioGeography", "Sector", "Units",
      "Indicator", "Scenario", "Sub_Technology", "Year", "Direction", "mktFSRatio", "techFSRatio",
      "FairSharePerc"
    ) %in% colnames(data)
  )

  stopifnot(data_has_expected_columns)

  data <- data %>%
    dplyr::filter(
      (stringr::str_detect(.data$Source, "WEO2023") & .data$Indicator %in% c("Capacity", "Total energy supply")
      )) %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio
      )
    ) %>%
    dplyr::rename(
      scenario_source = .data$Source,
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    ) %>%
    dplyr::relocate(
      .data$scenario_source, .data$scenario_geography, .data$scenario,
      .data$ald_sector, .data$units, .data$technology, .data$year,
      .data$direction, .data$fair_share_perc
    ) %>%
    dplyr::mutate(
      scenario = stringr::str_c(.data$scenario_source, .data$scenario, sep = "_")
    ) %>%
    dplyr::distinct_all()

  # We use Only Global Region for now so no more wrangling needed

  data <- data %>%
    dplyr::select(-.data$scenario_source)

  return(data)
}

#' Scenario processing for weo23
#' This script takes preprocessed data from weo23 and wrangles it
#' Only global data for now
#' @param data data that contains the scenario data file that is to be
#'   processed


prepare_scenario_data_weo23 <- function(data) {
  data_has_expected_columns <- all(
    c(
      "Source", "Technology", "ScenarioGeography", "Sector", "Units", "scenario_pathway",
      "Indicator", "Scenario", "Sub_Technology", "Year", "Direction", "mktFSRatio", "techFSRatio",
      "FairSharePerc"
    ) %in% colnames(data)
  )

  stopifnot(data_has_expected_columns)


  # due to inconsistencies in the raw data across sources, we need to filter for
  # other Indicators in IEA scenarios than in GECO scenarios at least up until
  # WEO 2021 and GECO 2021. Please review once new scenarios are available
  data <- data %>%
    dplyr::filter(
      !(.data$Technology == "RenewablesCap" & !is.na(.data$Sub_Technology)) # THIS NEEDS TO BE INVESTIGATED AS SUBTECHNOLOGIES CURRENTLY ALWAYS EMPTY
    ) %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio
      )
    ) %>%
    dplyr::rename(
      scenario_source = .data$Source,
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    ) %>%
    dplyr::relocate(
      .data$scenario_source, .data$scenario_geography, .data$scenario,
      .data$ald_sector, .data$units, .data$technology, .data$year,
      .data$direction, .data$fair_share_perc, .data$scenario_pathway
    ) %>%
    dplyr::mutate(
      scenario = stringr::str_c(.data$scenario_source, .data$scenario, sep = "_")
    ) %>%
    dplyr::distinct_all()

  # removing sectors that are not supported by stress testing
  p4i_p4b_sector_technology_lookup_df <- p4i_p4b_sector_technology_lookup()

  data <- data %>%
    dplyr::filter(.data$ald_sector %in% unique(p4i_p4b_sector_technology_lookup_df$sector_p4i))

  data <- remove_incomplete_sectors(data)

  data <- data %>%
    dplyr::select(-.data$scenario_source)

  return(data)
}


#' Function reads GECO2023 raw automotive data and wrangles it into the
#' required Format.
#'@param data The dataset to be processed. This should be a data frame that
#' includes the necessary columns for processing.
#'
#' @return A data frame with the processed GECO 2023 data.


prepare_geco2023 <- function(data) {
  # renaming and wrangling

  data<- data %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio
      )
    ) %>%
    dplyr::rename(
      scenario_source = .data$Source,
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    ) %>%
    dplyr::relocate(
      .data$scenario_source, .data$scenario_geography, .data$scenario,
      .data$ald_sector, .data$units, .data$technology, .data$year,
      .data$direction, .data$fair_share_perc
    ) %>%
    dplyr::mutate(
      scenario = stringr::str_c(.data$scenario_source, .data$scenario, sep = "_")
    ) %>%
    dplyr::distinct_all()

  # removing source column and renaming world into global

  data <- data %>%
    dplyr::select(-.data$scenario_source) %>%
    dplyr::mutate(scenario_geography = ifelse(.data$scenario_geography == "World", "Global", .data$scenario_geography))

}


#' This function reads  NGFS raw scenario data from Phase IV as found in the dropbox
#' under "Processed Data" and wrangles it to fit the required format for the
#' usual scenario analysis input routine.
#'
#' @param data Tibble that contains the scenario data file that is to be
#' @param start_year
#'   processed
#' @family data preparation functions
#' @export

preprepare_ngfs_scenario_data <- function(data, start_year) {

  data <- data %>%
    dplyr::mutate(scenario = .data$Scenario) %>%
    dplyr::mutate(
      scenario = dplyr::case_when(
        .data$scenario == "Nationally Determined Contributions (NDCs)" ~ "NDC",
        .data$scenario == "Below 2 C" ~ "B2DS",
        .data$scenario == "Delayed transition" ~ "DT",
        .data$scenario == "Current Policies" ~ "CP",
        .data$scenario == "Divergent Net Zero" ~ "DN0",
        .data$scenario == "Net Zero 2050" ~ "NZ2050",
        .data$scenario == "Fragmented World" ~ "FW",
        .data$scenario == "Low demand" ~ "LD",
        TRUE ~ .data$scenario
      ),
      scenario_geography = dplyr::case_when(
        .data$Region == "World" ~ "Global",
        TRUE ~ .data$Region
      ),
      sector = dplyr::case_when(
        .data$category_b == "Oil" ~ "Oil&Gas",
        .data$category_b == "Gas" ~ "Oil&Gas",
        .data$category_b == "Coal" ~ "Coal",
        TRUE ~ "Power"
      ),
      technology = dplyr::case_when(
        .data$category_b == "Oil" ~ "Oil",
        .data$category_b == "Gas" ~ "Gas",
        .data$category_b == "Coal" ~ "Coal",
        .data$category_b == "Electricity" & .data$category_c == "Coal" ~ "CoalCap",
        .data$category_b == "Electricity" & .data$category_c == "Gas" ~ "GasCap",
        .data$category_b == "Electricity" & .data$category_c == "Hydro" ~ "HydroCap",
        .data$category_b == "Electricity" & .data$category_c == "Nuclear" ~ "NuclearCap",
        .data$category_b == "Electricity" & .data$category_c == "Oil" ~ "OilCap",
        .data$category_b == "Electricity" & .data$category_c == "Solar" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Geothermal" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Biomass" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Wind" ~ "RenewablesCap",
        TRUE ~ .data$category_c
      ),
      indicator = dplyr::if_else(
        .data$sector == "Power", "Capacity", "Production"
      ),
      source = paste("NGFS", start_year, sep = ""),
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.2-4.6" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 1.1-M-R12" ~ "MESSAGE",
        TRUE ~ .data$Model
      )
    ) %>%
    dplyr::rename(units = .data$Unit) %>%
    dplyr::ungroup() %>%  ##think it needs to be ungrouped here
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_c, .data$category_a, .data$category_b, .data$Region))


  combine_renewables_cap <- data %>%
    dplyr::filter(.data$technology == "RenewablesCap") %>%
    dplyr::group_by(.data$year, .data$technology, .data$scenario_geography, .data$model, .data$scenario) %>%
    dplyr::mutate(value = sum(.data$value)) %>%
    unique()

  delete_renewables <- data %>% dplyr::filter(!.data$technology %in% c("RenewablesCap"))

  data <- dplyr::full_join(combine_renewables_cap, delete_renewables) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2023", .data$scenario))
}


style_ngfs <- function(data) {

  data <- data %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio, .data$Source
      )
    ) %>%
    dplyr::rename(
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    )

}

#' This function reads  NGFS raw scenario from Phase V

preprepare_ngfs_scenario_data_v5 <- function(data) {

  #scenario trajectory based on capacity or electricity. One has to be blocked out with
  # a "#". For now we use capacity
  #data <- data[!grepl("^Capacity\\|Electricity\\|", data$Variable), ]
  data <- data[!grepl("^Secondary Energy\\|Electricity\\|", data$Variable), ]

  #setting the vintage
  vintage <- 2024

  data <- data %>%
    dplyr::mutate(scenario = .data$Scenario) %>%
    dplyr::mutate(
      scenario = dplyr::case_when(
        .data$scenario == "Nationally Determined Contributions (NDCs)" ~ "NDC",
        .data$scenario == "Below 2 C" ~ "B2DS",
        .data$scenario == "Delayed transition" ~ "DT",
        .data$scenario == "Current Policies" ~ "CP",
        .data$scenario == "Divergent Net Zero" ~ "DN0",
        .data$scenario == "Net Zero 2050" ~ "NZ2050",
        .data$scenario == "Fragmented World" ~ "FW",
        .data$scenario == "Low demand" ~ "LD",
        TRUE ~ .data$scenario
      ),
      scenario_geography = dplyr::case_when(
        .data$Region == "World" ~ "Global",
        TRUE ~ .data$Region
      ),
      sector = dplyr::case_when(
        .data$category_b == "Oil" ~ "Oil&Gas",
        .data$category_b == "Gas" ~ "Oil&Gas",
        .data$category_b == "Coal" ~ "Coal",
        .data$category_b == "Cement" ~ "Cement", #adding cement
        TRUE ~ "Power"
      ),
      technology = dplyr::case_when(
        .data$category_b == "Oil" ~ "Oil",
        .data$category_b == "Gas" ~ "Gas",
        .data$category_b == "Coal" ~ "Coal",
        .data$category_b == "Cement" ~ "Cement",
        .data$category_b == "Electricity" & .data$category_c == "Coal" ~ "CoalCap",
        .data$category_b == "Electricity" & .data$category_c == "Gas" ~ "GasCap",
        .data$category_b == "Electricity" & .data$category_c == "Hydro" ~ "HydroCap",
        .data$category_b == "Electricity" & .data$category_c == "Nuclear" ~ "NuclearCap",
        .data$category_b == "Electricity" & .data$category_c == "Oil" ~ "OilCap",
        .data$category_b == "Electricity" & .data$category_c == "Solar" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Geothermal" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Biomass" ~ "RenewablesCap",
        .data$category_b == "Electricity" & .data$category_c == "Wind" ~ "RenewablesCap",
        TRUE ~ .data$category_c
      ),
      # Adding an extra condition of indicator. if we use production for the trajectory for the power sector
      indicator = if_else(
        sector == "Power" & grepl("^Capacity\\|Electricity\\|", Variable), "Capacity",
        if_else(sector == "Power" & grepl("^Secondary Energy\\|Electricity\\|", Variable), "Production",
                "Production")),
      source = paste("NGFS", vintage, sep = ""),
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.3-4.8" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 2.0-M-R12-NGFS" ~ "MESSAGE",
        TRUE ~ .data$Model
      )
    ) %>%
    dplyr::rename(units = .data$Unit) %>%
    dplyr::ungroup() %>%  ##think it needs to be ungrouped here
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_c, .data$category_a, .data$category_b, .data$Region))


  combine_renewables_cap <- data %>%
    dplyr::filter(.data$technology == "RenewablesCap") %>%
    dplyr::group_by(.data$year, .data$technology, .data$scenario_geography, .data$model, .data$scenario) %>%
    dplyr::mutate(value = sum(.data$value)) %>%
    unique()

  delete_renewables <- data %>% dplyr::filter(!.data$technology == "RenewablesCap")

  data <- dplyr::full_join(combine_renewables_cap, delete_renewables) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2024", .data$scenario))
}


#### IPR Scenario Analysis Function
#### Prepares Scenario Analysis Input for IPR using the usual routine
prepare_IPR_scenario_data2023 <- function(data, start_year) {
  ### Creating a technology column

  data$technology <- ifelse(data$Sector == "Power",
                            paste(data$Sub_variable_class_2, data$Sector, sep = "_"),
                            ifelse(data$Sector == "Transport",
                                   data$Sub_variable_class_2,
                                   data$Sub_variable_class_1))

  ### Renaming technologies and Sector

  data <- data %>%
    dplyr::rename(ald_sector = .data$Sector) %>%
    dplyr::mutate(technology = .data$technology) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Oil" ~ "Oil",
        .data$technology == "Coal" ~ "Coal",
        .data$technology == "Natural gas" ~ "Gas",
        .data$technology == "Coal_Power" ~ "CoalCap",
        .data$technology == "Natural gas_Power" ~ "GasCap",
        .data$technology == "Oil_Power" ~ "OilCap",
        .data$technology == "Nuclear_Power" ~ "NuclearCap",
        .data$technology == "Hydro_Power" ~ "HydroCap",
        .data$technology == "Biomass_Power" ~ "BiomassCap", ### Is this the same as Biomass?
        .data$technology == "Offshore wind_Power" ~ "OffWindCap",
        .data$technology == "Onshore wind_Power" ~ "OnWindCap",
        .data$technology == "Solar_Power" ~ "SolarCap",
        .data$technology == "BEV" ~ "Electric",
        .data$technology == "PHEV" ~ "Hybrid",
        .data$technology == "H2" ~ "FuelCell",
        .data$technology == "ICE" ~ "ICE"
      ),
      ald_sector = dplyr::case_when(
        .data$technology == "Oil" ~ "Oil&Gas",
        .data$technology == "Gas" ~ "Oil&Gas",
        .data$technology == "Coal" ~ "Coal",
        .data$technology == "CoalCap" ~ "Power",
        .data$technology == "GasCap" ~ "Power",
        .data$technology == "OilCap" ~ "Power",
        .data$technology == "NuclearCap" ~ "Power",
        .data$technology == "HydroCap" ~ "Power",
        .data$technology == "BiomassCap" ~ "Power",
        .data$technology == "OffWindCap" ~ "Power",
        .data$technology == "OnWindCap" ~ "Power",
        .data$technology == "SolarCap" ~ "Power",
        .data$technology == "ICE" ~ "Automotive",
        .data$technology == "Electric" ~ "Automotive",
        .data$technology == "Hybrid" ~ "Automotive",
        .data$technology == "FuelCell" ~ "Automotive",
      ),
      Scenario = dplyr::case_when(
        .data$Scenario == "RPS" ~ "IPR2023_RPS",
        .data$Scenario == "FPS" & .data$ald_sector != "Automotive" ~ "IPR2023_FPS",
        .data$Scenario == "FPS" & .data$ald_sector == "Automotive" ~ "IPR2023Automotive_FPS"
      )
    )


  ### Renaming Region WORLD to Global

  data <- data %>%
    dplyr::mutate(Region = ifelse(.data$Region == "WORLD", "Global", .data$Region))

  ### Deleting all NAs, NAs exist because the current data still has data that we are currently not using, like hydrogen and Coal with CCS

  data <- data[!(is.na(data$ald_sector)), ]

  ### further deleting unnecessary columns

  data <- dplyr::select(data, -c("Variable_class", "Sub_variable_class_1", "Sub_variable_class_2"))

  ### renaming column names

  colnames(data)[colnames(data) == "Scenario"] <- "scenario"
  colnames(data)[colnames(data) == "Region"] <- "scenario_geography"
  colnames(data)[colnames(data) == "Units"] <- "units"

  ### creating Renewablescap

  combine_Renewablecap <- data[data$technology == "OffWindCap" | data$technology == "OnWindCap" | data$technology == "SolarCap" | data$technology == "BiomassCap", ]

  combine_Renewablecap <- combine_Renewablecap %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$year) %>%
    dplyr::summarize(value = sum(.data$value)) %>%
    dplyr::ungroup()

  combine_Renewablecap$technology <- "RenewablesCap"

  ### binding RenewablesCap with main data

  data <- rbind(data, combine_Renewablecap)

  ### Deleting Offwind, Onwind, Solar and Biomass, to avoid double counting
  data <- data[!(data$technology == "OffWindCap" | data$technology == "OnWindCap" | data$technology == "SolarCap" | data$technology == "BiomassCap"), ]

  ### Calculating TMSR

  data$year <- as.numeric(as.character(data$year))
  data <- data[!(data$year < start_year), ]

  data <- data %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$technology) %>%
    dplyr::arrange(data$year, .by_group = TRUE) %>%
    dplyr::mutate(tmsr = (.data$value - dplyr::first(.data$value)) / dplyr::first(.data$value))


  ### Calculating SMSP

  data <- data %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$year) %>%
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    dplyr::mutate(sector_total_by_year = sum(.data$value)) %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$technology) %>%
    dplyr::mutate(
      smsp = (.data$value - dplyr::first(.data$value)) /
        dplyr::first(.data$sector_total_by_year),
      sector_total_by_year = NULL
    ) %>%
    dplyr::ungroup()


  ### Green Techs, Direction and FairSharePerc
  ### Defines direction of technology based on whether its considered a green technology

  green_techs <- c("RenewablesCap", "HydroCap", "NuclearCap", "SolarCap", "OffWindCap", "OnWindCap", "BiomassCap",
                   "Electric", "FuelCell") ##ADDING Automotive Electric and FuelCell. I believe Hybrid is better a declining technology

  data <- data %>%
    dplyr::mutate(
      direction = dplyr::if_else(.data$technology %in% green_techs, "increasing", "declining"),
      fair_share_perc = dplyr::if_else(.data$direction == "declining", .data$tmsr, .data$smsp)
    )

  data <- data[, c(
    "scenario_geography", "scenario", "ald_sector", "technology", "units", "year",
    "direction", "fair_share_perc", "value"
  )] %>%
  dplyr::rename(
    scenario_pathway=.data$value
  )

  #limiting time horizon for IPR automotive until 2041, this is the maximum data we currently have from
  #the GECO2021 scenarios

  data <- data %>%
    dplyr::filter(!(.data$ald_sector == "Automotive" & .data$year >= 2042))
}

## IPR Baseline Scenario part I
## builts upon the WEO2021 STEPS scenario
prepare_IPR_baseline_scenario <- function(data) {
  # takes WEO wrangled scenario data and outputs only baseline with the scenario renamed to IPR2021_baseline
  data <- data %>%
    dplyr::filter(.data$scenario == "WEO2021_STEPS") %>%
    dplyr::mutate(scenario = dplyr::case_when(
      .data$scenario == "WEO2021_STEPS" ~ "IPR2023_baseline"
    ))

  return(data)
}

## IPR Baseline Scenario part II Automotive
## builts upon the GECO2021 CurPol scenario
prepare_IPR_baseline_scenario_automotive <- function(data) {
  data_has_expected_columns <- all(
    c(
      "Source", "Technology", "ScenarioGeography", "Sector", "Units",
      "Indicator", "Scenario", "Sub_Technology", "Year", "Direction", "mktFSRatio", "techFSRatio",
      "FairSharePerc"
    ) %in% colnames(data)
  )

  stopifnot(data_has_expected_columns)

  data <- data %>%
    dplyr::filter(
      !(stringr::str_detect(.data$Source, "GECO2021") & .data$Sector != "Automotive")
    ) %>%
    dplyr::filter(
      !(.data$Technology == "RenewablesCap" & !is.na(.data$Sub_Technology)) # THIS NEEDS TO BE INVESTIGATED AS SUBTECHNOLOGIES CURRENTLY ALWAYS EMPTY
    ) %>%
    dplyr::select(
      -c(
        .data$Sub_Technology, .data$Indicator, .data$mktFSRatio, .data$techFSRatio
      )
    ) %>%
    dplyr::rename(
      scenario_source = .data$Source,
      scenario_geography = .data$ScenarioGeography,
      scenario = .data$Scenario,
      ald_sector = .data$Sector,
      units = .data$Units,
      technology = .data$Technology,
      year = .data$Year,
      direction = .data$Direction,
      fair_share_perc = .data$FairSharePerc
    ) %>%
    dplyr::relocate(
      .data$scenario_source, .data$scenario_geography, .data$scenario,
      .data$ald_sector, .data$units, .data$technology, .data$year,
      .data$direction, .data$fair_share_perc
    ) %>%
    dplyr::mutate(
      scenario = stringr::str_c(.data$scenario_source, .data$scenario, sep = "_")
    ) %>%
    dplyr::distinct_all()

  data <- data %>%
    dplyr::select(-.data$scenario_source)

  # final adjustment to IPR
  data <- data %>%
    dplyr::filter(.data$scenario == "GECO2021_CurPol") %>%
    dplyr::mutate(scenario = dplyr::case_when(
      .data$scenario == "GECO2021_CurPol" ~ "IPR2023Automotive_baseline"
    ))

  return(data)
}

### Prepare Oxford Scenario Data

prepare_OXF_scenario_data <- function(data, start_year) {
  ### Removing technologies that are not relevant for the stress test
  data <- data %>%
    dplyr::filter(!.data$`Annual energy` %in% c("batteries_ST_transport", "batteries_ST_electricity", "batteries_LT_electricity", "hydrogen"))

  ### Separating annual energy into sector and technology columns
  data <- data %>%
    tidyr::separate(col = "Annual energy", into = c("technology", "ald_sector"), sep = "_")

  data <- data %>%
    dplyr::mutate(
      ald_sector = dplyr::if_else(.data$ald_sector == "electricity", "Power", .data$ald_sector),
      ald_sector = dplyr::if_else(.data$technology == "coal" & .data$ald_sector == "final", "Coal", .data$ald_sector),
      ald_sector = dplyr::if_else(.data$technology == "gas" & .data$ald_sector == "final", "Oil&Gas", .data$ald_sector),
      ald_sector = dplyr::if_else(.data$technology == "oil" & .data$ald_sector == "final", "Oil&Gas", .data$ald_sector),
      technology = dplyr::if_else(.data$technology == "coal" & .data$ald_sector == "Power", "CoalCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "gas" & .data$ald_sector == "Power", "GasCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "oil" & .data$ald_sector == "Power", "OilCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "coal" & .data$ald_sector == "Coal", "Coal", .data$technology),
      technology = dplyr::if_else(.data$technology == "gas" & .data$ald_sector == "Oil&Gas", "Gas", .data$technology),
      technology = dplyr::if_else(.data$technology == "oil" & .data$ald_sector == "Oil&Gas", "Oil", .data$technology),
      technology = dplyr::if_else(.data$technology == "solar", "SolarCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "wind", "WindCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "nuclear", "NuclearCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "hydro", "HydroCap", .data$technology),
      technology = dplyr::if_else(.data$technology == "bioenergy", "BiomassCap", .data$technology)
    )


  ### creating Renewablescap

  combine_Renewablecap <- data[data$technology == "WindCap" | data$technology == "SolarCap" | data$technology == "BiomassCap", ]

  combine_Renewablecap <- combine_Renewablecap %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$year) %>%
    dplyr::summarize(value = sum(.data$value))

  combine_Renewablecap$technology <- "RenewablesCap"

  ### binding RenewablesCap with main data

  data <- rbind(data, combine_Renewablecap)

  ### Deleting Wind, Solar and Biomass, to avoid double counting
  data <- data[!(data$technology == "WindCap" | data$technology == "SolarCap" | data$technology == "BiomassCap"), ]

  ### Calculating TMSR

  #start_year <- 2021
  data$year <- as.numeric(as.character(data$year))
  data <- data[!(data$year < start_year), ]

  data <- data %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$technology) %>%
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    dplyr::mutate(tmsr = (.data$value - dplyr::first(.data$value)) / dplyr::first(.data$value))


  ### Calculating SMSP

  data <- data %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$year) %>%
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    dplyr::mutate(sector_total_by_year = sum(.data$value)) %>%
    dplyr::group_by(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$technology) %>%
    dplyr::mutate(
      smsp = (.data$value - dplyr::first(.data$value)) /
        dplyr::first(.data$sector_total_by_year),
      sector_total_by_year = NULL
    ) %>%
    dplyr::ungroup()

  ### Creating OilCap as duplicate of GasCap, note that we only introduce OilCap after the calculation of TMSR/SMSP

  subset <- data %>%
    dplyr::filter(.data$technology == "GasCap") %>%
    dplyr::mutate(technology = stringr::str_replace(.data$technology, "GasCap", "OilCap"))

  data <- dplyr::bind_rows(subset, data)

  ### Green Techs, Direction and FairSharePerc
  ### Defines direction of technology based on whether its considered a green technology

  green_techs <- c("RenewablesCap", "HydroCap", "NuclearCap", "SolarCap", "WindCap", "BiomassCap")

  data <- data %>%
    dplyr::mutate(
      direction = dplyr::if_else(.data$technology %in% green_techs, "increasing", "declining"),
      fair_share_perc = dplyr::if_else(.data$direction == "declining", .data$tmsr, .data$smsp),
      tmsr = NULL,
      smsp = NULL,
    )

  data <- data[, c(
    "scenario_geography", "scenario", "ald_sector", "technology", "units", "year",
    "direction", "fair_share_perc", "value"
  )]
  return(data)
}

## Prepare Steel Scenario Data
prepare_steel_scenario_data <- function(data, start_year, max_year=2050) {

  data <- data %>%
    dplyr::rename(value = "Production (Mt)") %>% # renaming
    dplyr::filter(.data$technology %in% c('Avg BF-BOF', 'DRI-Melt-BOF', 'EAF', 'DRI-EAF'))%>% # selecting relevant technologies
    dplyr::filter(.data$scenario %in% c('Baseline', 'Carbon Cost')) # selecting relevant scenarios

  # creates rows for missing yearly observations
  data <- data %>%
    # Ensure the year column is treated as a numeric column
    dplyr::mutate(year = as.numeric(as.character(.data$year))) %>%
    # Complete missing combinations of scenario, technology, and year
    tidyr::complete(.data$scenario, .data$technology, year = start_year:max_year) %>%
    # arrange the dataset for easier reading
    dplyr::arrange(.data$scenario, .data$technology, .data$year)

  data$scenario_geography <- "Global"
  data$sector <- "Steel"

  # rename  specified technologies
  data_renamed <- data %>%
    dplyr:: mutate(technology = dplyr::case_when(
      .data$technology == "Avg BF-BOF" ~ "BF-BOF",
      .data$technology == "DRI-Melt-BOF" ~ "DRI-BOF",
      .data$technology == "DRI-EAF" ~ "DRI-EAF",
      TRUE ~ technology
    ))

  # Filter rows where technology is "EAF" and duplicate with renamed technology "BF-EAF"
  eaf_to_bf_eaf <- data_renamed %>%
    dplyr::filter(.data$technology == "EAF") %>%
    dplyr::mutate(technology = "BF-EAF")


  # Combine the modified datasets back with the original
  data <- data_renamed %>%
    dplyr::bind_rows(eaf_to_bf_eaf)


  ## some technologies "BOF_BF" have NAs in the last years, presumable beause the value goes to 0
  ## replacing NAs for these technologies with 0

  data <- data %>%
    # Group by scenario and technology to treat each combination individually
    dplyr::group_by(.data$scenario, .data$technology) %>%
    # Arrange by year within each group to ensure chronological order
    dplyr::arrange(.data$year, .by_group = TRUE) %>%
    # Apply a custom replacement for trailing NAs within each group
    dplyr::mutate(
      # Identify the last year with a non-NA value for each group
      last_value_year = max(.data$year[!is.na(.data$value)], na.rm = TRUE),
      # Replace NA with 0 in years following the last non-NA year
      value = ifelse(.data$year > .data$last_value_year, 0, .data$value)
    ) %>%
    # Remove the temporary column used for calculation
    dplyr::select(-.data$last_value_year) %>%
    # Ungroup to remove the grouping structure
    dplyr::ungroup()

  ##
  # BOF-DRI has missing values in the first years of the time series
  # I think for now assuming the first value as a constant for the previous NA is a reasonable approach
  # with this we can simulate the no growth trajectory we would have when we would use 0s and also create the reasonable growth trajectory
  # without using smsp--Using SMSP can heavily alter the results in this case, especially if a company has no production except in one technology
  # (waiting for feedback from MP on this)

  # Prepare the replacement value
  replacement_value <- data %>%
    dplyr::filter(.data$year == 2026, .data$technology == "DRI-BOF") %>%
    dplyr::select(.data$scenario, value_2026 = .data$value) %>%
    dplyr::distinct()

  data <- data %>%
    dplyr::left_join(replacement_value, by = "scenario") %>%
    dplyr::mutate(value = dplyr::if_else(.data$technology == "DRI-BOF" & is.na(.data$value) & !is.na(.data$value_2026), .data$value_2026, .data$value)) %>%
    dplyr::select(-.data$value_2026) # Clean up, remove the helper column

  ##add missing technologies in BOF
  # Filter rows where technology is "BF-BOF" and rename to "BOF"
  bf_bof_to_bof <- data %>%
    dplyr::filter(.data$technology == "BF-BOF") %>%
    dplyr::mutate(technology = "BOF")

  # Filter rows where technology is "BF-BOF" and rename to "BF-OHF"
  bf_bof_to_bf_ohf <- data %>%
    dplyr::filter(.data$technology == "BF-BOF") %>%
    dplyr::mutate(technology = "BF-OHF")

  data <- data %>%
    dplyr::bind_rows(bf_bof_to_bof, bf_bof_to_bf_ohf)

  # calculate tmsr smsp
  # I advice against smsp for steel, since it is quite the signficant adjustment and I am unsure about the feasibility of
  # aggregating the sector like that
  # I assume now only tsmr
  data <- data %>%
    dplyr::filter(.data$year >= start_year) %>%
    add_market_share_columns(start_year = start_year)

  # Adding a direction column
  # First iteration: I put all directions as decreasing and use tsmr for all
  # (decreasing only used to choose between tmsr and smsp and for carbon tax purposes)
  # there are some differences in the overall trajectory direction, but this should be captured by "overshoot_direction"
  # for information: the decreasing techs are:
  # BOF-BF
  # EAF-DRI

  data$direction <- "declining"
  data$units <- "Mt/yr"

  data$fair_share_perc <- data$tmsr

  data <- data %>%
    dplyr::rename(ald_sector = .data$sector)

  # Select and rearrange columns
  data <- data %>%
    dplyr::select(.data$scenario_geography, .data$scenario, .data$ald_sector, .data$technology, .data$units, .data$year, .data$direction, .data$fair_share_perc, .data$value) %>%
    dplyr::rename(scenario_pathway=.data$value)

  # Rename scenarios
  data <- data %>%
    dplyr::mutate(scenario = dplyr::case_when(
      .data$scenario == "Baseline" ~ "Steel_baseline",
      .data$scenario == "Carbon Cost" ~ "Steel_NZ",
      TRUE ~ scenario  # Keeps all other values as they are
    ))
}
