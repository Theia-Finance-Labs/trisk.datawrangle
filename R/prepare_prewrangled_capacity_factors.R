#' This function reads scenario data in the form of data as found in the dropbox
#' under "Raw Data", extracts the relevant information for power capacity factors
#' and prepares these in the format required in the stress test. Currently
#' expects to get data for up to year 2040.
#'
#' @param data Tibble that provides raw scenario data file that is to be
#' @param start_year Beginning of the analysis.
#'   processed
#' @family data preparation functions
#' @return NULL

prepare_prewrangled_capacity_factors_WEO2021 <- function(data, start_year) {
  # WEO2021 start year should be the release year
  #start_year <- 2021

  # WEO2021 end year in raw data is 2040 as it os based on weo2020 at the moment
  end_year <- 2040

  # incompatible with 2017 and 2018 datasets
  data_has_expected_columns <- all(
    c(
      "Source", "Indicator", "Sector", "Units", "Scenario", "ScenarioGeography",
      "Technology", "Sub_Technology", as.character(end_year)
    ) %in% colnames(data)
  )
  stopifnot(data_has_expected_columns)

  hours_to_year <- 24 * 365

  data <- data %>%
    dplyr::filter(
      .data$Sector == "Power",
      .data$Technology != "Total capacity"
    )

  data <- data %>%
    dplyr::filter(
      (.data$Technology != "Renewables" & is.na(.data$Sub_Technology)) |
        (.data$Technology == "Renewables" & !is.na(.data$Sub_Technology))
    )

  data <- data %>%
    dplyr::mutate(
      Technology = dplyr::if_else(
        .data$Technology == "Renewables" & .data$Sub_Technology == "Hydro",
        "Hydro",
        .data$Technology
      )
    )

  data <- data %>%
    dplyr::select(
      .data$Source, .data$Indicator, .data$Sector, .data$Units, .data$Scenario,
      .data$ScenarioGeography, .data$Technology, dplyr::starts_with("20")
    ) %>%
    dplyr::group_by(
      .data$Source, .data$Indicator, .data$Sector, .data$Units, .data$Scenario,
      .data$ScenarioGeography, .data$Technology
    ) %>%
    dplyr::summarise(
      dplyr::across(
        dplyr::starts_with("20"),
        ~ sum(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) %>%
    dplyr::ungroup()

  capacity <- data %>%
    dplyr::filter(.data$Indicator == "Capacity") %>%
    dplyr::rename(units = .data$Units) %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("20"),
      names_to = "year",
      values_to = "capacity"
    ) %>%
    dplyr::mutate(
      year = as.integer(.data$year)
    ) %>%
    dplyr::select(
      .data$Source, .data$Scenario, .data$ScenarioGeography, .data$Sector,
      .data$Technology, .data$year, .data$units, .data$capacity
    )

  generation <- data %>%
    dplyr::filter(.data$Indicator == "Generation") %>%
    dplyr::rename(units = .data$Units) %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("20"),
      names_to = "year",
      values_to = "generation"
    ) %>%
    dplyr::mutate(
      year = as.integer(.data$year)
    ) %>%
    dplyr::select(
      .data$Source, .data$Scenario, .data$ScenarioGeography, .data$Sector,
      .data$Technology, .data$year, .data$units, .data$generation
    ) %>%
    dplyr::mutate(
      generation = .data$generation * 1000 / .env$hours_to_year,
      units = "GW"
    )

  capacity_factors <- generation %>%
    dplyr::inner_join(
      capacity,
      by = c(
        "Source", "Scenario", "ScenarioGeography", "Sector", "Technology", "units", "year"
      )
    ) %>%
    dplyr::distinct_all() %>%
    tidyr::complete(
      year = seq(.env$start_year, .env$end_year),
      tidyr::nesting(
        !!!rlang::syms(
          c("Source", "Scenario", "ScenarioGeography", "Sector", "Technology", "units")
        )
      )
    ) %>%
    dplyr::arrange(
      .data$Source, .data$Scenario, .data$ScenarioGeography, .data$Sector,
      .data$Technology, .data$units, .data$year
    ) %>%
    dplyr::group_by(
      .data$Source, .data$Scenario, .data$ScenarioGeography, .data$Sector,
      .data$Technology, .data$units
    ) %>%
    dplyr::mutate(
      # interpolate missing values using linear interpolation to avoid problems
      # with unrealistic lower/upper bounds
      capacity = zoo::na.approx(object = .data$capacity),
      generation = zoo::na.approx(object = .data$generation)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(.data$year >= .env$start_year)

  capacity_factors <- capacity_factors %>%
    dplyr::mutate(capacity_factor = .data$generation / .data$capacity) %>%
    # if both capacity and generation are 0, we get capacity factor NaN. Until
    # we have clarity on how to best handle this, we assume capacity factor 0
    # in such a a case
    dplyr::mutate(
      capacity_factor = dplyr::if_else(
        is.na(.data$capacity_factor),
        0,
        .data$capacity_factor
      )
    ) %>%
    dplyr::rename(
      source = .data$Source,
      scenario = .data$Scenario,
      # TODO: check if region and region_2dii are somehow required. see prior version
      scenario_geography = .data$ScenarioGeography,
      ald_sector = .data$Sector,
      technology = .data$Technology
    ) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Coal" ~ "CoalCap",
        .data$technology == "Oil" ~ "OilCap",
        .data$technology == "Natural gas" ~ "GasCap",
        .data$technology == "Hydro" ~ "HydroCap",
        .data$technology == "Nuclear" ~ "NuclearCap",
        .data$technology == "Renewables" ~ "RenewablesCap",
        TRUE ~ .data$technology
      )
    ) %>%
    dplyr::mutate(
      scenario_geography = dplyr::if_else(
        .data$scenario_geography == "World",
        "Global",
        .data$scenario_geography
      )
    )

  capacity_factors_has_nas <- any(is.na(capacity_factors$capacity_factor))
  if (capacity_factors_has_nas) {
    stop("Data must not contain capacity factors with NA values.", call. = FALSE)
  }

  capacity_factors_out_of_bounds <- min(
    capacity_factors$capacity_factor,
    na.rm = TRUE
  ) < 0 |
    max(capacity_factors$capacity_factor, na.rm = TRUE) > 1
  if (capacity_factors_out_of_bounds) {
    stop(
      "Capacity factors with values below 0 or greater than 1 in data. This is
      not logically possible. Please check input data and fix.",
      call. = FALSE
    )
  }

  capacity_factors <- capacity_factors %>%
    dplyr::select(
      .data$scenario, .data$scenario_geography, .data$technology, .data$year,
      .data$capacity_factor
    )

  output_has_expected_columns <- all(
    c(
      "scenario", "scenario_geography", "technology", "year", "capacity_factor"
    ) %in% colnames(capacity_factors)
  )
  stopifnot(output_has_expected_columns)

  capacity_factors <- capacity_factors %>%
    dplyr::mutate(ald_sector = "Power") %>%
    # Power is the only currently included sector
    remove_incomplete_sectors() %>%
    dplyr::select(-.data$ald_sector) %>%
    dplyr::mutate(scenario = paste("WEO2021", .data$scenario, sep = "_"))

  # adjustments of weo2020 raw data to fiot weo2021 scenario
  capacity_factors <- capacity_factors %>%
    dplyr::mutate(
      scenario = dplyr::if_else(
        .data$scenario == "WEO2021_SPS",
        "WEO2021_STEPS",
        .data$scenario
      )
    )

  capacity_factors_nze <- capacity_factors %>%
    dplyr::filter(.data$scenario == "WEO2021_SDS") %>%
    dplyr::mutate(scenario = "WEO2021_NZE_2050")

  capacity_factors_aps <- capacity_factors %>%
    dplyr::filter(.data$scenario == "WEO2021_STEPS") %>%
    dplyr::mutate(scenario = "WEO2021_APS")

  capacity_factors <- dplyr::bind_rows(
    capacity_factors,
    capacity_factors_nze,
    capacity_factors_aps
  )

  capacity_factors
}

#' WEO2023
#' This function reads prewrangled weo2023 capacity and net generation data for APS, STEPS and NZ scenarios
#' under the Global geography
#' @param data A dataframe containing the preprocessed data that needs to be wrangled.
#'             Expected to have columns for sector, indicator, value, source,
#'             scenario, scenario_geography, technology, year, and units.
#' @param start_year The starting year from which capacity factors should be calculated.

prepare_prewrangled_capacity_factors_WEO2023 <- function(data, start_year) {

  hours_to_year <- 24 * 365
  end_year = 2050

  data <- data %>%
    dplyr::filter(
      .data$sector == "Power"
    )

  capacity <- data %>%
    dplyr::filter(.data$indicator == "Capacity") %>%
    dplyr::rename(capacity = .data$value)%>%
    dplyr::select(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$year, .data$units, .data$capacity
    )

  generation <- data %>%
    dplyr::filter(.data$indicator == "Electricity generation") %>%
    dplyr::rename(generation = .data$value)%>%
    dplyr::select(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$year, .data$units, .data$generation
    ) %>%
    dplyr::mutate(
      generation = .data$generation * 1000 / .env$hours_to_year,
      units = "GW"
    )

  capacity_factors <- generation %>%
    dplyr::inner_join(
      capacity,
      by = c(
        "source", "scenario", "scenario_geography", "sector", "technology", "units", "year"
      )
    ) %>%
    dplyr::distinct_all() %>%
    tidyr::complete(
      year = seq(.env$start_year, .env$end_year),
      tidyr::nesting(
        !!!rlang::syms(
          c("source", "scenario", "scenario_geography", "sector", "technology", "units")
        )
      )
    ) %>%
    dplyr::arrange(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$units, .data$year
    ) %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$units
    ) %>%
    dplyr::mutate(
      # interpolate missing values using linear interpolation to avoid problems
      # with unrealistic lower/upper bounds
      capacity = zoo::na.approx(object = .data$capacity),
      generation = zoo::na.approx(object = .data$generation)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(.data$year >= .env$start_year)

  capacity_factors <- capacity_factors %>%
    dplyr::mutate(capacity_factor = .data$generation / .data$capacity) %>%
    # if both capacity and generation are 0, we get capacity factor NaN. Until
    # we have clarity on how to best handle this, we assume capacity factor 0
    # in such a a case
    dplyr::mutate(
      capacity_factor = dplyr::if_else(
        is.na(.data$capacity_factor),
        0,
        .data$capacity_factor
      )
    )

  capacity_factors_has_nas <- any(is.na(capacity_factors$capacity_factor))
  if (capacity_factors_has_nas) {
    stop("Data must not contain capacity factors with NA values.", call. = FALSE)
  }

  capacity_factors_out_of_bounds <- min(
    capacity_factors$capacity_factor,
    na.rm = TRUE
  ) < 0 |
    max(capacity_factors$capacity_factor, na.rm = TRUE) > 1
  if (capacity_factors_out_of_bounds) {
    stop(
      "Capacity factors with values below 0 or greater than 1 in data. This is
      not logically possible. Please check input data and fix.",
      call. = FALSE
    )
  }

  capacity_factors <- capacity_factors %>%
    dplyr::select(
      .data$scenario, .data$scenario_geography, .data$technology, .data$year,
      .data$capacity_factor
    )

  output_has_expected_columns <- all(
    c(
      "scenario", "scenario_geography", "technology", "year", "capacity_factor"
    ) %in% colnames(capacity_factors)
  )
  stopifnot(output_has_expected_columns)

  capacity_factors <- capacity_factors %>%
    dplyr::mutate(ald_sector = "Power") %>%
    # Power is the only currently included sector
    remove_incomplete_sectors() %>%
    dplyr::select(-.data$ald_sector) %>%
    dplyr::mutate(scenario = paste("WEO2023", .data$scenario, sep = "_"))

  capacity_factors
}


#' This function reads in raw ngfs capacity and secondary energy data, as found in the dropbox
#' under "Raw Data" and creates capacity factors for the power sector.
#' and prepares these in the format required in the stress test.
#'
#' @param data Tibble that provides raw capacity and secondary energy ngfs data file that is to be
#'   processed
#' @param start_year Beginning of the analysis.
#'
#' @family data preparation functions
#' @return NULL

# NGFS Phase IV
prepare_capacity_factors_NGFS2023 <- function(data, start_year) {

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
      # NOTE: rename World to Global to fit the ST scenario geography names
      scenario_geography = dplyr::case_when(
        .data$Region == "World" ~ "Global",
        TRUE ~ .data$Region
      ),
      technology = dplyr::case_when(
        .data$category_c == "Oil" ~ "OilCap",
        .data$category_c == "Gas" ~ "GasCap",
        .data$category_c == "Coal" ~ "CoalCap",
        .data$category_c == "Hydro" ~ "HydroCap",
        .data$category_c == "Nuclear" ~ "NuclearCap",
        .data$category_c == "Oil" ~ "OilCap",
        .data$category_c == "Solar" ~ "RenewablesCap",
        .data$category_c == "Geothermal" ~ "RenewablesCap",
        .data$category_c == "Biomass" ~ "RenewablesCap",
        .data$category_c == "Wind" ~ "RenewablesCap",
        TRUE ~ .data$category_c
      ),
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.2-4.6" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 1.1-M-R12" ~ "MESSAGE",
        TRUE ~ .data$Model
      )
    ) %>%
    dplyr::rename(units = .data$Unit) %>%
    dplyr::ungroup()%>%
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_b, .data$category_c, .data$Region))


  combine_renewables <- data %>%
    dplyr::filter(.data$technology == "RenewablesCap") %>%
    dplyr::group_by(.data$year, .data$technology, .data$scenario_geography, .data$model, .data$scenario, .data$category_a) %>%
    dplyr::mutate(value = sum(.data$value)) %>%
    unique()

  delete_renewables <- data %>% dplyr::filter(!.data$technology == "RenewablesCap")

  data <- dplyr::full_join(combine_renewables, delete_renewables)

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$year, .data$value))) %>%
    tidyr::complete(year = tidyr::full_seq(.data$year, 1)) %>%
    dplyr::mutate(
      value = zoo::na.approx(.data$value, .data$year, na.rm = FALSE)
    ) %>%
    dplyr::ungroup()

  data <- data %>% dplyr::filter(.data$year >= start_year)

  generation <- data %>%
    dplyr::filter(.data$category_a == "Secondary Energy") %>%
    dplyr::mutate(
      value = .data$value * 31.68808781,
      units = "GW"
    ) %>%
    tidyr::pivot_wider(
      names_from = .data$category_a,
      values_from = .data$value
    ) %>%
    dplyr::rename(generation = .data$`Secondary Energy`)

  capacity <- data %>%
    dplyr::filter(.data$category_a == "Capacity") %>%
    tidyr::pivot_wider(
      names_from = .data$category_a,
      values_from = .data$value
    ) %>%
    dplyr::rename(capacity = .data$Capacity)

  data <- dplyr::full_join(capacity, generation)

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$generation, .data$capacity))) %>%
    dplyr::mutate(capacity_factor = as.double(.data$generation) / as.double(.data$capacity)) %>%
    dplyr::ungroup()

  data <- data %>%
    # if capacity factor is bigger than 1 make it 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity_factor > 1, 1, .data$capacity_factor)
    ) %>%
    # if capacity is 0 and generation is bigger than 0, it  results in INF capacity factors, which we correct to a capacity factor of 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation > 0, 0, .data$capacity_factor)
    ) %>%
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation > 0, 0, .data$capacity_factor)
    ) %>%
    # if both capacity and generation are 0, we get capacity factor NaN. Until
    # we have clarity on how to best handle this, we assume capacity factor 0
    # in such a a case
    dplyr::mutate(capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation == 0, 0, .data$capacity_factor))

  data <- data %>%
    dplyr::select(-c(.data$capacity, .data$generation, .data$units)) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2023", .data$scenario))
}

### NGFS Phase V 2024

prepare_capacity_factors_NGFS2024 <- function(data, start_year) {

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
      # NOTE: rename World to Global to fit the ST scenario geography names
      scenario_geography = dplyr::case_when(
        .data$Region == "World" ~ "Global",
        TRUE ~ .data$Region
      ),
      technology = dplyr::case_when(
        .data$category_c == "Oil" ~ "OilCap",
        .data$category_c == "Gas" ~ "GasCap",
        .data$category_c == "Coal" ~ "CoalCap",
        .data$category_c == "Hydro" ~ "HydroCap",
        .data$category_c == "Nuclear" ~ "NuclearCap",
        .data$category_c == "Oil" ~ "OilCap",
        .data$category_c == "Solar" ~ "RenewablesCap",
        .data$category_c == "Geothermal" ~ "RenewablesCap",
        .data$category_c == "Biomass" ~ "RenewablesCap",
        .data$category_c == "Wind" ~ "RenewablesCap",
        TRUE ~ .data$category_c
      ),
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.3-4.8" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 2.0-M-R12-NGFS" ~ "MESSAGE",
        TRUE ~ .data$Model
      )
    ) %>%
    dplyr::rename(units = .data$Unit) %>%
    dplyr::ungroup()%>%
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_b, .data$category_c, .data$Region))


  combine_renewables <- data %>%
    dplyr::filter(.data$technology == "RenewablesCap") %>%
    dplyr::group_by(.data$year, .data$technology, .data$scenario_geography, .data$model, .data$scenario, .data$category_a) %>%
    dplyr::mutate(value = sum(.data$value)) %>%
    unique()

  delete_renewables <- data %>% dplyr::filter(!.data$technology == "RenewablesCap")

  data <- dplyr::full_join(combine_renewables, delete_renewables)

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$year, .data$value))) %>%
    tidyr::complete(year = tidyr::full_seq(.data$year, 1)) %>%
    dplyr::mutate(
      value = zoo::na.approx(.data$value, .data$year, na.rm = FALSE)
    ) %>%
    dplyr::ungroup()

  data <- data %>% dplyr::filter(.data$year >= start_year)

  generation <- data %>%
    dplyr::filter(.data$category_a == "Secondary Energy") %>%
    dplyr::mutate(
      value = .data$value * 31.68808781,
      units = "GW"
    ) %>%
    tidyr::pivot_wider(
      names_from = .data$category_a,
      values_from = .data$value
    ) %>%
    dplyr::rename(generation = .data$`Secondary Energy`)

  capacity <- data %>%
    dplyr::filter(.data$category_a == "Capacity") %>%
    tidyr::pivot_wider(
      names_from = .data$category_a,
      values_from = .data$value
    ) %>%
    dplyr::rename(capacity = .data$Capacity)

  data <- dplyr::full_join(capacity, generation)

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$generation, .data$capacity))) %>%
    dplyr::mutate(capacity_factor = as.double(.data$generation) / as.double(.data$capacity)) %>%
    dplyr::ungroup()

  data <- data %>%
    # if capacity factor is bigger than 1 make it 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity_factor > 1, 1, .data$capacity_factor)
    ) %>%
    # if capacity is 0 and generation is bigger than 0, it  results in INF capacity factors, which we correct to a capacity factor of 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation > 0, 0, .data$capacity_factor)
    ) %>%
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation > 0, 0, .data$capacity_factor)
    ) %>%
    # if both capacity and generation are 0, we get capacity factor NaN. Until
    # we have clarity on how to best handle this, we assume capacity factor 0
    # in such a a case
    dplyr::mutate(capacity_factor = dplyr::if_else(.data$capacity == 0 & .data$generation == 0, 0, .data$capacity_factor))

  data <- data %>%
    dplyr::select(-c(.data$capacity, .data$generation, .data$units)) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2024", .data$scenario))
}

### IPR Capacity Factors
prepare_capacity_factors_IPR2023 <- function(data, start_year) {
  ### Creating a technology column

  data$technology <- ifelse(data$Sector == "Power", paste(data$Sub_variable_class_2, data$Sector, sep = "_"), data$Sub_variable_class_1)
  data$technology <- ifelse(data$Variable_class == "Electricity generation", paste(data$Sub_variable_class_1, data$Sector, sep = "_"), data$technology)

  ### Renaming sector and technology

  data <- data %>%
    dplyr::rename(ald_sector = .data$Sector, Category = .data$Variable_class) %>%
    dplyr::mutate(technology = .data$technology) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Coal_Power" ~ "CoalCap",
        .data$technology == "Natural gas_Power" ~ "GasCap",
        .data$technology == "Nuclear_Power" ~ "NuclearCap",
        .data$technology == "Hydro_Power" ~ "HydroCap",
        .data$technology == "Oil_Power" ~ "OilCap",
        .data$technology == "Biomass_Power" ~ "BiomassCap", ### Is this the same as Biomass?
        .data$technology == "Offshore wind_Power" ~ "OffWindCap",
        .data$technology == "Onshore wind_Power" ~ "OnWindCap",
        .data$technology == "Solar_Power" ~ "SolarCap"
      ),
      ald_sector = dplyr::case_when(
        .data$technology == "CoalCap" ~ "Power",
        .data$technology == "GasCap" ~ "Power",
        .data$technology == "OilCap" ~ "Power",
        .data$technology == "NuclearCap" ~ "Power",
        .data$technology == "HydroCap" ~ "Power",
        .data$technology == "BiomassCap" ~ "Power",
        .data$technology == "OffWindCap" ~ "Power",
        .data$technology == "OnWindCap" ~ "Power",
        .data$technology == "SolarCap" ~ "Power"
      ),
      Scenario = dplyr::case_when(
        .data$Scenario == "RPS" ~ "IPR2023_RPS",
        .data$Scenario == "FPS" ~ "IPR2023_FPS"
      )
    )

  ## Renaming Region WORLD to Global

  data <- data %>%
    dplyr::mutate(Region = ifelse(.data$Region == "WORLD", "Global", .data$Region))

  ### deleting all NAs, NAs exist because the current data still has data that we are currently
  ### not using, like hydrogen and Coal w/ CCS.

  data <- data[!(is.na(data$ald_sector)), ]

  ### further deleting unnecessary columns

  data <- dplyr::select(data, -c("Sub_variable_class_1", "Sub_variable_class_2"))

  ### renaming column names

  data <- data %>%
    dplyr::rename(
      scenario = .data$Scenario,
      scenario_geography = .data$Region,
      units = .data$Units
    )

  ### creating Renewablescap

  combine_RenewablesCap <- data[data$technology == "OffWindCap" | data$technology == "OnWindCap" | data$technology == "SolarCap" | data$technology == "BiomassCap", ]

  combine_RenewablesCap <- combine_RenewablesCap %>%
    dplyr::group_by(.data$Category, .data$scenario_geography, .data$scenario, .data$ald_sector, .data$units, .data$year) %>%
    dplyr::summarize(value = sum(.data$value))

  combine_RenewablesCap$technology <- "RenewablesCap"

  ### binding RenewablesCap with data

  data <- rbind(data, combine_RenewablesCap)

  ### Creating data sets for Capacity and Generation. They will be merged again at a later stage

  Capacity <- data[data$Category == "Capacity", ]
  Generation <- data[data$Category == "Electricity generation", ]

  ### Generation: Transforming TWH into GW

  Generation <- Generation %>%
    dplyr::mutate(
      value = Generation$value * 1000 / (24 * 365.25),
      units = "GW"
    )

  ### renaming colnames

  colnames(Generation)[colnames(Generation) == "value"] <- "Generation"
  colnames(Capacity)[colnames(Capacity) == "value"] <- "Capacity"

  ### deleting unneccessary column for the full_join

  Generation <- dplyr::select(Generation, -c("Category"))
  Capacity <- dplyr::select(Capacity, -c("Category"))

  ### joining the data

  data <- dplyr::full_join(Capacity, Generation)

  ### Calculating Capacity Factors

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$Generation, .data$Capacity))) %>%
    dplyr::mutate(capacity_factor = as.double(.data$Generation) / as.double(.data$Capacity)) %>%
    dplyr::ungroup()

  ### there are a couple Capacity factors above 1 and a couple that are NA. since
  ### the capacity and generation are both 0. Here we adjust capacity factors similar to NGFS

  data <- data %>%
    # if capacity factor is bigger than 1 make it 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$capacity_factor > 1, 1, .data$capacity_factor)
    ) %>%
    # if capacity is 0 and generation is bigger than 0, it  results in INF capacity factors, which we correct to a capacity factor of 1
    dplyr::mutate(
      capacity_factor = dplyr::if_else(.data$Capacity == 0 & .data$Generation > 0, 0, .data$capacity_factor)
    ) %>%
    # if both capacity and generation are 0, we get capacity factor NaN. Until
    # we have clarity on how to best handle this, we assume capacity factor 0
    # in such a a case
    dplyr::mutate(capacity_factor = dplyr::if_else(.data$Capacity == 0 & .data$Generation == 0, 0, .data$capacity_factor))

  data <- data %>%
    dplyr::select(-c(.data$Capacity, .data$Generation, .data$units, .data$ald_sector))

  ### filtering for start year
  data$year <- as.numeric(as.character(data$year))
  data <- data %>% dplyr::filter(.data$year >= start_year)
}



# IPR Baseline Scenario Capacity Factors
# IPR Baseline is a duplicate of IPR2023_FPS capacity factors

prepare_capacity_factors_IPR2023_baseline <- function(data) {
  data <- data %>%
    dplyr::filter(.data$scenario == "IPR2023_FPS") %>%
    dplyr::mutate(scenario = dplyr::case_when(
      .data$scenario == "IPR2023_FPS" ~ "IPR2023_baseline"
    ))
}
### Oxford Capacity Factors

prepare_capacity_factors_OXF2021 <- function(data) {
  ### Oxford has no capacity factors. Hence we rely on IEA capacity facotors which we match to Oxford scenarios
  ### Oxford Capacity Factors are then held constant from 2040 until 2100.

  # IEA wrangling and matching to OXF

  data <- data %>%
    dplyr::filter(
      .data$scenario_geography == "Global",
      .data$scenario %in% c("WEO2021_SDS", "WEO2021_STEPS")
    ) %>%
    dplyr::mutate(scenario = ifelse(.data$scenario == "WEO2021_SDS", "Oxford2021_fast", ifelse(.data$scenario == "WEO2021_STEPS", "Oxford2021_base", .data$scenario)))


  # Function to add the years from 2040 to 2100

  add_years <- function(data, start, end) {
    technologies <- unique(data$technology)
    scenarios <- unique(data$scenario)
    scenario_geography <- unique(data$scenario_geography)
    new_data <- data
    for (year in start:end) {
      for (technology in technologies) {
        for (scenario in scenarios) {
          new_row <- data.frame(scenario_geography = scenario_geography, year = year, capacity_factor = NA, technology = technology, scenario = scenario, stringsAsFactors = FALSE)
          new_data <- rbind(new_data, new_row)
        }
      }
    }
    return(new_data)
  }


  data <- add_years(data, 2041, 2100)


  # replace NAs with values from 2040 for each scenario-geography-technology combination
  for (scn in unique(data$scenario)) {
    for (tech in unique(data$technology)) {
      subset <- data[data$scenario == scn & data$scenario_geography == "Global" & data$technology == tech, ]
      subset$capacity_factor[is.na(subset$capacity_factor)] <- subset$capacity_factor[subset$year == 2040]
      data[data$scenario == scn & data$scenario_geography == "Global" & data$technology == tech, ] <- subset
    }
  }
  return(data)
}


### Steel Capacity Factors
prepare_capacity_factors_GEM_steel <- function(data, start_year, max_year=2050){

  # adding scenario geography
  data$scenario_geography <- "Global"

  # Renaming BOF Steel directly
  data <- data %>%
    dplyr::mutate(technology = dplyr::case_when(
      .data$technology == "BOF Steel" ~ "BOF-BF",
      TRUE ~ technology
    ))

  # Step 2: Create duplicates for EAF Steel with three new names, and DRI with two new names
  eaf_bf <- data %>%
    dplyr::filter(.data$technology == "EAF Steel") %>%
    dplyr::mutate(technology = "EAF-BF")

  eaf_ohf <- data %>%
    dplyr::filter(.data$technology == "EAF Steel") %>%
    dplyr::mutate(technology = "EAF-OHF")

  eaf_mm <- data %>%
    dplyr::filter(.data$technology == "EAF Steel") %>%
    dplyr::mutate(technology = "EAF-MM")

  bof_dri <- data %>%
    dplyr::filter(.data$technology == "DRI") %>%
    dplyr::mutate(technology = "BOF-DRI")

  eaf_dri <- data %>%
    dplyr::filter(.data$technology == "DRI") %>%
    dplyr::mutate(technology = "EAF-DRI")

  # Combining the new duplicated datasets back with the original, excluding original EAF Steel and DRI rows
  data <- data %>%
    dplyr::filter(!(.data$technology %in% c("EAF Steel", "DRI"))) %>%
    dplyr::bind_rows(eaf_bf, eaf_ohf, eaf_mm, bof_dri, eaf_dri)

  # Step 3: Duplicate all observations for the scenario column
  data <- data %>%
    dplyr::mutate(scenario = "Steel_baseline") %>%
    dplyr::bind_rows(dplyr::mutate(data, scenario = "Steel_NZ"))

  # Expand dataset first without attempting to fill 'value'
  data <- data %>%
    dplyr::group_by(.data$technology, .data$scenario, .data$scenario_geography) %>%
    tidyr::complete(year = start_year:max_year) %>%
    dplyr::ungroup()

  # This step assumes uses the first non-NA 'value' for each 'technology', 'scenario', 'scenario_geography' combination
  data <- data %>%
    dplyr::group_by(.data$technology, .data$scenario, .data$scenario_geography) %>%
    dplyr::mutate(value = ifelse(is.na(.data$value), dplyr::first(.data$value[!is.na(.data$value)]), .data$value)) %>%
    dplyr::ungroup()

  # filtering for only relevant technologies
  data <- data %>%
    dplyr::filter(.data$technology %in% c("BOF-BF", "BOF-DRI", "EAF-BF", "EAF-DRI", "EAF-OHF", "EAF-MM"))

  #renaming value column
  data <- data %>%
    dplyr:: rename(capacity_factor = .data$value)
}
