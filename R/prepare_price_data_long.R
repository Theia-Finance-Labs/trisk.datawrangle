#' This function reads in multiple price data files from WEO2021 and wrangles
#' it to fit the format to be used in the transition risk stress test work flow.
#'
#' @param input_data_fossil_fuel Tibble containing the raw price data for fossil
#'   fuels
#' @param input_data_power Tibble containing the raw price/LCOE data for power
#'
#' @family data preparation functions
#'
#' @export
prepare_price_data_long_WEO2021 <- function(input_data_fossil_fuel,
                                            input_data_power) {
  # the WEO 2020 raw data has values for 2019, but not for 2020. So the first year
  # must be 2019, if data prior to 2025 (next value) are to be interpolated.
  first_year <- 2020

  fossil_fuels_data_has_expected_columns <- all(
    c(
      "source", "sector", "unit", "scenario_geography", "scenario"
    ) %in% colnames(input_data_fossil_fuel)
  )
  stopifnot(fossil_fuels_data_has_expected_columns)

  fossil_fuels_data <- input_data_fossil_fuel %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("20"),
      names_to = "year",
      values_to = "price"
    ) %>%
    dplyr::mutate(
      year = as.numeric(.data$year)
    ) %>%
    dplyr::filter(.data$year >= .env$first_year)

  # map price units to AR production units
  # coal: tonnes of coal -> tonnes of coal (already correct)
  # Oil & Gas: barrel and Mbtu -> GJ
  fossil_fuels_data <- fossil_fuels_data %>%
    dplyr::mutate(
      price = dplyr::case_when(
        .data$unit == "usd/barrel" ~ 0.16 * .data$price,
        .data$unit == "usd/Mbtu" ~ 0.9478171203 * .data$price,
        TRUE ~ .data$price
      )
    ) %>%
    dplyr::mutate(
      unit = dplyr::case_when(
        .data$unit == "usd/barrel" ~ "GJ",
        .data$unit == "usd/Mbtu" ~ "GJ",
        TRUE ~ .data$unit
      )
    )

  fossil_fuels_data <- fossil_fuels_data %>%
    # ADO 1192: technology is wrongly called sector in raw data. rename
    dplyr::rename(technology = .data$sector) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Crude oil" ~ "Oil",
        .data$technology == "IEA crude oil" ~ "Oil",
        .data$technology == "Natural gas" ~ "Gas",
        TRUE ~ .data$technology
      )
    ) %>%
    dplyr::mutate(
      sector = dplyr::if_else(
        .data$technology == "Coal",
        "Coal",
        "Oil&Gas"
      )
    ) %>%
    dplyr::mutate(
      indicator = "price"
    )

  # ADO 1192 - for some technologies, there are no global price data.
  # approximate with simple mean based on all other given regions
  fossil_fuels_data_global <- fossil_fuels_data %>%
    dplyr::filter(.data$technology %in% c("Gas", "Coal")) %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$sector, .data$technology, .data$year,
      .data$unit, .data$indicator
    ) %>%
    dplyr::summarise(
      price = mean(.data$price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(scenario_geography = "Global")

  fossil_fuels_data <- fossil_fuels_data %>%
    dplyr::bind_rows(fossil_fuels_data_global)

  power_data_has_expected_columns <- all(
    c(
      "source", "scenario", "region", "technology", "indicator", "unit"
    ) %in% colnames(input_data_power)
  )
  stopifnot(power_data_has_expected_columns)

  power_data <- input_data_power %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("20"),
      names_to = "year",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      year = as.numeric(.data$year)
    ) %>%
    dplyr::filter(.data$year >= .env$first_year)

  power_data <- power_data %>%
    dplyr::filter(.data$indicator == "LCOE") %>%
    dplyr::rename(
      scenario_geography = .data$region,
      price = .data$value
    ) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Nuclear" ~ "NuclearCap",
        .data$technology == "Coal" ~ "CoalCap",
        .data$technology == "Gas CCGT" ~ "GasCap",
        TRUE ~ "RenewablesCap"
      )
    ) %>%
    dplyr::mutate(
      sector = "Power"
    ) %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$unit, .data$year, .data$indicator
    ) %>%
    # ado 1192 - summarise so that every technology only has one row left per
    # combination. Because of combining multiple wind and solar techs from the
    # raw data into "RenewablesCap" in the processed data, this is not initially
    # the case
    dplyr::summarise(
      price = mean(.data$price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(names(fossil_fuels_data))

  missing_power_data <- power_data %>%
    dplyr::filter(.data$technology %in% c("GasCap", "RenewablesCap")) %>%
    # ADO 1192 - use gascap and renwablescap data for oilcap and hydrocap
    # as a placeholder until IEA replies
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "GasCap" ~ "OilCap",
        .data$technology == "RenewablesCap" ~ "HydroCap",
        TRUE ~ .data$technology
      )
    )

  power_data <- power_data %>%
    dplyr::bind_rows(missing_power_data)

  power_data <- power_data %>%
    dplyr::group_by(
      .data$source, .data$technology, .data$unit, .data$scenario_geography,
      .data$scenario, .data$sector, .data$indicator
    ) %>%
    dplyr::arrange(
      .data$source, .data$technology, .data$unit, .data$scenario_geography,
      .data$scenario, .data$sector, .data$indicator, .data$year
    ) %>%
    tidyr::fill("price", .direction = "down") %>%
    dplyr::ungroup()


  # ADO 1192 - there are no global price data for any of the power technologies
  # approximate with simple mean based on all other given regions
  power_data_global <- power_data %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$year, .data$sector, .data$technology,
      .data$unit, .data$indicator
    ) %>%
    dplyr::summarise(
      price = mean(.data$price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(scenario_geography = "Global")

  power_data <- power_data %>%
    dplyr::bind_rows(power_data_global)

  data <- fossil_fuels_data %>%
    dplyr::bind_rows(power_data) %>%
    dplyr::relocate(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit, .data$year, .data$price
    )

  min_year <- min(data$year, na.rm = TRUE)
  max_year <- max(data$year, na.rm = TRUE)

  data <- data %>%
    tidyr::complete(
      year = seq(.env$min_year, .env$max_year),
      tidyr::nesting(
        !!!rlang::syms(
          c(
            "source", "scenario", "scenario_geography", "sector", "technology",
            "indicator", "unit"
          )
        )
      )
    ) %>%
    dplyr::arrange(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit, .data$year
    )

  data <- data %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit
    ) %>%
    dplyr::mutate(price = zoo::na.approx(object = .data$price)) %>%
    dplyr::ungroup()

  data_has_nas <- any(is.na(data$price))
  if (data_has_nas) {
    stop("Data must not contain prices with NA values.", call. = FALSE)
  }

  min_price_greater_equal_zero <- min(data$price, na.rm = TRUE) >= 0
  stopifnot(min_price_greater_equal_zero)

  return(data)
}

#' This function reads raw NGFS price data files from Phase 4 in and wrangles
#' it to fit the format to be used in the transition risk stress test work flow.
#'
#' @param input_data_fossil_fuels_ngfs A dataset containing prices for the fossil
#' fuels
#' @param start_year Beginning year of the analysis.
#'
#' @family data preparation functions
#'
#' @export


prepare_price_data_long_NGFS2023 <- function(input_data_fossil_fuels_ngfs, start_year) {
  data <- input_data_fossil_fuels_ngfs %>%
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
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.2-4.6" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 1.1-M-R12" ~ "MESSAGE",
        TRUE ~ .data$Model
      ),
      sector = dplyr::case_when(
        .data$category_c == "Oil" ~ "Oil&Gas",
        .data$category_c == "Gas" ~ "Oil&Gas",
        .data$category_c == "Coal" ~ "Coal",
        TRUE ~ .data$category_c
      )
    ) %>%
    dplyr::rename(unit = .data$Unit, technology = .data$category_c, indicator = .data$category_a) %>%
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_b, .data$Region))

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$year, .data$value))) %>%
    tidyr::complete(year = tidyr::full_seq(.data$year, 1)) %>%
    dplyr::mutate(
      value = zoo::na.approx(.data$value, .data$year, na.rm = FALSE)
    ) %>%
    dplyr::ungroup()

  data <- data %>% dplyr::filter(.data$year >= start_year)

  data_oil_gas <- data %>%
    dplyr::filter(.data$sector == "Oil&Gas") %>%
    dplyr::mutate(unit = "$/GJ")

  data_coal <- data %>%
    dplyr::filter(.data$sector == "Coal") %>%
    dplyr::group_by(.data$year, .data$scenario_geography, .data$model, .data$scenario) %>%
    dplyr::mutate(value = .data$value / 0.03414368, unit = "$/tonnes")

  data <- dplyr::full_join(data_oil_gas, data_coal)

  data <- data %>%
    dplyr::rename(price = .data$value) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2023", .data$scenario))
}

### Preparing Price Data Long for NGFS Phase 5

prepare_price_data_long_NGFS2024 <- function(input_data_fossil_fuels_ngfsv5, start_year) {
  data <- input_data_fossil_fuels_ngfsv5 %>%
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
      model = dplyr::case_when(
        .data$Model == "GCAM 6.0 NGFS" ~ "GCAM",
        .data$Model == "REMIND-MAgPIE 3.3-4.8" ~ "REMIND",
        .data$Model == "MESSAGEix-GLOBIOM 2.0-M-R12-NGFS" ~ "MESSAGE",
        TRUE ~ .data$Model
      ),
      sector = dplyr::case_when(
        .data$category_c == "Oil" ~ "Oil&Gas",
        .data$category_c == "Gas" ~ "Oil&Gas",
        .data$category_c == "Coal" ~ "Coal",
        TRUE ~ .data$category_c
      )
    ) %>%
    dplyr::rename(unit = .data$Unit, technology = .data$category_c, indicator = .data$category_a) %>%
    dplyr::select(-c(.data$Model, .data$Variable, .data$Scenario, .data$category_b, .data$Region))

  data <- data %>%
    dplyr::group_by(dplyr::across(-c(.data$year, .data$value))) %>%
    tidyr::complete(year = tidyr::full_seq(.data$year, 1)) %>%
    dplyr::mutate(
      value = zoo::na.approx(.data$value, .data$year, na.rm = FALSE)
    ) %>%
    dplyr::ungroup()

  data <- data %>% dplyr::filter(.data$year >= start_year) ##is this necessary?

  data_oil_gas <- data %>%
    dplyr::filter(.data$sector == "Oil&Gas") %>%
    dplyr::mutate(unit = "$/GJ")

  data_coal <- data %>%
    dplyr::filter(.data$sector == "Coal") %>%
    dplyr::group_by(.data$year, .data$scenario_geography, .data$model, .data$scenario) %>%
    dplyr::mutate(value = .data$value / 0.03414368, unit = "$/tonnes")

  data <- dplyr::full_join(data_oil_gas, data_coal)

  data <- data %>%
    dplyr::rename(price = .data$value) %>%
    tidyr::unite("scenario", c(.data$model, .data$scenario), sep = "_") %>%
    dplyr::mutate(scenario = paste0("NGFS2024", .data$scenario))
}


### IPR price data function

prepare_price_data_long_IPR2023 <- function(data, start_year) {
  ### Objective: extract the prices for Oil coal and Gas
  # Coal: only available for Europe, USA; CHN and JPN. We take the average to get a global variable
  # Gas: only available for USA, Europe and Asia, Plus available as high price and low price. We create a global low and global high and take the average from that
  # Oil: available only for World and as high and low price. We take the average
  # Unit: We transform in the correct unit for the ST

  ### Creating a technology column

  data$technology <- data$Sub_variable_class_1

  ### Renaming technologies and Sector

  data <- data %>%
    dplyr::mutate(technology = .data$technology) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Oil" ~ "Oil",
        .data$technology == "Coal" ~ "Coal",
        .data$technology == "Natural gas" ~ "Gas",
      ),
      sector = dplyr::case_when(
        .data$technology == "Oil" ~ "Oil&Gas",
        .data$technology == "Gas" ~ "Oil&Gas",
        .data$technology == "Coal" ~ "Coal",
      ),
      Scenario = dplyr::case_when(
        .data$Scenario == "RPS" ~ "IPR2023_RPS",
        .data$Scenario == "FPS" ~ "IPR2023_FPS"
      )
    )

  ### further deleting unnecessary columns

  data <- dplyr::select(data, -c("Sub_variable_class_1"))

  ### renaming column names

  data <- data %>%
    dplyr::rename(
      scenario = .data$Scenario,
      scenario_geography = .data$Region,
      unit = .data$Units,
      price = .data$value
    )


  ### Creating Coal Prices
  coal_global <- data %>%
    dplyr::filter(.data$technology == "Coal") %>%
    dplyr::group_by(.data$scenario, .data$Variable_class, .data$year) %>%
    dplyr::summarize(price = mean(.data$price)) %>%
    dplyr::mutate(Variable_class = "price", scenario_geography = "Global", sector = "Coal", technology = "Coal", unit = "USD / tonne")

  ### Creating Global Gas prices for High and Low
  gas_global <- data %>%
    dplyr::filter(.data$technology == "Gas") %>%
    dplyr::group_by(.data$scenario, .data$Variable_class, .data$year) %>%
    dplyr::summarize(price = mean(.data$price)) %>%
    dplyr::mutate(scenario_geography = "Global", sector = "Oil&Gas", technology = "Gas", unit = "USD / MMBtu")

  ### Creating Average of the high and low prices
  gas_global <- gas_global %>%
    dplyr::group_by(.data$scenario, .data$year) %>%
    dplyr::summarize(price = mean(.data$price), Variable_class = "price", scenario_geography = "Global", sector = "Oil&Gas", technology = "Gas", unit = "USD / MMBtu")

  ### Creating an average of the Oil technology high and low price per scenario and year
  oil_avg <- data %>%
    dplyr::filter(.data$technology == "Oil") %>%
    dplyr::group_by(.data$scenario, .data$year) %>%
    dplyr::summarize(price = mean(.data$price), Variable_class = "price", scenario_geography = "Global", sector = "Oil&Gas", technology = "Oil", unit = "USD / Barrel")

  data <- rbind(coal_global, gas_global, oil_avg) ### For now we only take global prices from IPR

  ### Unit Adjustment: We use $/GJ for Oil and Gas. For coal we use $/tonne

  data <- data %>%
    dplyr::mutate(
      price = dplyr::case_when(
        .data$unit == "USD / Barrel" ~ 0.16 * .data$price,
        .data$unit == "USD / MMBtu" ~ 0.9478171203 * .data$price,
        TRUE ~ .data$price
      )
    ) %>%
    dplyr::mutate(
      unit = dplyr::case_when(
        .data$unit == "USD / Barrel" ~ "GJ",
        .data$unit == "USD / MMBtu" ~ "GJ",
        .data$unit == "USD / tonne" ~ "usd/tonne"
      )
    )

  ### renaming Variable_Class to "indicator"

  colnames(data)[which(colnames(data) == "Variable_class")] <- "indicator"

  ### filtering for start year
  data$year <- as.numeric(as.character(data$year))
  data <- data %>% dplyr::filter(.data$year >= start_year)

  return(data)
}


### Function reading LCOE data for IPR2021
### For IPR we are using WEO2021 LCOE for the power prices
### Function below reads in WEO2021 LCOE data (similar to the WEO2021 function above, but without the section covering fossil fuel data)
### Output of the function is then matched to IPR with a different function (prepare_lcoe_adjusted_price_data_IPR2021)which can be found
### in Prepare_LCOE_adjusted_price_data.R

prepare_price_data_long_Power_IPR2023 <- function(input_data_power) {
  first_year <- 2020
  power_data_has_expected_columns <- all(
    c(
      "source", "scenario", "region", "technology", "indicator", "unit"
    ) %in% colnames(input_data_power)
  )
  stopifnot(power_data_has_expected_columns)

  power_data <- input_data_power %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with("20"),
      names_to = "year",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      year = as.numeric(.data$year)
    ) %>%
    dplyr::filter(.data$year >= .env$first_year)

  power_data <- power_data %>%
    dplyr::filter(.data$indicator == "LCOE") %>%
    dplyr::rename(
      scenario_geography = .data$region,
      price = .data$value
    ) %>%
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "Nuclear" ~ "NuclearCap",
        .data$technology == "Coal" ~ "CoalCap",
        .data$technology == "Gas CCGT" ~ "GasCap",
        TRUE ~ "RenewablesCap"
      )
    ) %>%
    dplyr::mutate(
      sector = "Power"
    ) %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$unit, .data$year, .data$indicator
    ) %>%
    # ado 1192 - summarise so that every technology only has one row left per
    # combination. Because of combining multiple wind and solar techs from the
    # raw data into "RenewablesCap" in the processed data, this is not initially
    # the case
    dplyr::summarise(
      price = mean(.data$price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::ungroup()

  missing_power_data <- power_data %>%
    dplyr::filter(.data$technology %in% c("GasCap", "RenewablesCap")) %>%
    # ADO 1192 - use gascap and renwablescap data for oilcap and hydrocap
    # as a placeholder until IEA replies
    dplyr::mutate(
      technology = dplyr::case_when(
        .data$technology == "GasCap" ~ "OilCap",
        .data$technology == "RenewablesCap" ~ "HydroCap",
        TRUE ~ .data$technology
      )
    )

  power_data <- power_data %>%
    dplyr::bind_rows(missing_power_data)

  power_data <- power_data %>%
    dplyr::group_by(
      .data$source, .data$technology, .data$unit, .data$scenario_geography,
      .data$scenario, .data$sector, .data$indicator
    ) %>%
    dplyr::arrange(
      .data$source, .data$technology, .data$unit, .data$scenario_geography,
      .data$scenario, .data$sector, .data$indicator, .data$year
    ) %>%
    tidyr::fill("price", .direction = "down") %>%
    dplyr::ungroup()


  # ADO 1192 - there are no global price data for any of the power technologies
  # approximate with simple mean based on all other given regions
  power_data_global <- power_data %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$year, .data$sector, .data$technology,
      .data$unit, .data$indicator
    ) %>%
    dplyr::summarise(
      price = mean(.data$price, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(scenario_geography = "Global")

  power_data <- power_data %>%
    dplyr::bind_rows(power_data_global)
  data <- power_data %>%
    dplyr::relocate(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit, .data$year, .data$price
    )

  min_year <- min(data$year, na.rm = TRUE)
  max_year <- max(data$year, na.rm = TRUE)

  data <- data %>%
    tidyr::complete(
      year = seq(.env$min_year, .env$max_year),
      tidyr::nesting(
        !!!rlang::syms(
          c(
            "source", "scenario", "scenario_geography", "sector", "technology",
            "indicator", "unit"
          )
        )
      )
    ) %>%
    dplyr::arrange(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit, .data$year
    )

  data <- data %>%
    dplyr::group_by(
      .data$source, .data$scenario, .data$scenario_geography, .data$sector,
      .data$technology, .data$indicator, .data$unit
    ) %>%
    dplyr::mutate(price = zoo::na.approx(object = .data$price)) %>%
    dplyr::ungroup()

  data_has_nas <- any(is.na(data$price))
  if (data_has_nas) {
    stop("Data must not contain prices with NA values.", call. = FALSE)
  }

  min_price_greater_equal_zero <- min(data$price, na.rm = TRUE) >= 0
  stopifnot(min_price_greater_equal_zero)

  return(data)
}

## IPR baseline price data
## IPR baseline data is a duplicate of WEO2021 STEPS data

prepare_price_data_long_IPR2023_baseline <- function(data) {
  data <- data %>%
    dplyr::filter(.data$scenario == "WEO2021_STEPS") %>%
    dplyr::mutate(scenario = dplyr::case_when(
      .data$scenario == "WEO2021_STEPS" ~ "IPR2023_baseline"
    ))
}

## Oxford Fossil Fuel Price data function


prepare_price_data_long_Oxf2021 <- function(data, start_year) {
  ### Objective: extract the prices for Oil coal and Gas
  # All technologies only available for "Global" region
  # Units in $/MWH, have to be transfered into $/GJ for Oil and Gas and $/tonnes for coal


  ### Renaming technologies and Sector

  data <- data %>%
    dplyr::rename(
      technology = .data$Technology,
      sector = .data$Sector,
      scenario = .data$Scenario,
      scenario_geography = .data$Region,
      year = .data$Year,
      price = .data$LCOE
    )

  ### Time Horizon for Prices is limited until 2069, but Production data goes until 2100. We keep prices constant after 2069 until 2100

  # Function to add the years from 2069 to 2100
  add_years <- function(data, start, end) {
    technologies <- unique(data$technology)
    scenarios <- unique(data$scenario)
    scenario_geography <- unique(data$scenario_geography)
    new_data <- data
    for (year in start:end) {
      for (technology in technologies) {
        for (scenario in scenarios) {
          new_row <- data.frame(scenario_geography = scenario_geography, year = year, price = NA, technology = technology, scenario = scenario, sector = "Fossil Fuels", stringsAsFactors = FALSE)
          new_data <- rbind(new_data, new_row)
        }
      }
    }
    return(new_data)
  }

  data <- add_years(data, 2070, 2100)



  ## mutating data
  data <- data %>%
    dplyr::mutate(
      scenario = dplyr::case_when(
        .data$scenario == "Oxford - fast_transition" ~ "Oxford2021_fast",
        .data$scenario == "Oxford - no_transition" ~ "Oxford2021_base",
        .data$scenario == "Oxford - slow_transition" ~ "Oxford2021_slow"
      ),
      sector = dplyr::case_when(
        .data$technology == "Coal" ~ "Coal",
        .data$technology %in% c("Gas", "Oil") ~ "Oil&Gas",
      ),
      scenario_geography = dplyr::case_when(
        .data$scenario_geography == "World" ~ "Global"
      )
    )



  ## creating unit column
  data$unit <- "$/MWh"

  ## creating indicator column
  data$indicator <- "price"

  ## To convert Coal $/MWh into $/tonne, we need to divide the price/MWH by 0.122835 (1 MWh = 0.122835)
  ## To convert Oil and Gas $/MWH into GJ, we need to divide by 3.6 (1MWh = 3.6GJ)

  data <- data %>%
    dplyr::mutate(
      price = dplyr::if_else(.data$technology == "Oil", .data$price / 3.6, .data$price),
      price = dplyr::if_else(.data$technology == "Gas", .data$price / 3.6, .data$price),
      price = dplyr::if_else(.data$technology == "Coal", .data$price / 0.122835, .data$price),
      unit = dplyr::if_else(.data$technology == "Oil", "GJ", .data$unit),
      unit = dplyr::if_else(.data$technology == "Gas", "GJ", .data$unit),
      unit = dplyr::if_else(.data$technology == "Coal", "usd/tonne", .data$unit),
    )

  # delete data for years below 2021 and the Oxford_slow scenario
  data <- data %>%
    dplyr::filter(.data$year >= start_year) %>%
    dplyr::filter(.data$scenario != "Oxford2021_slow")


  # replace NAs with linear extrapolated values from the last 20 years of observation
  # for each scenario-geography-technology combination

  for (i in unique(data$technology)) {
    for (j in unique(data$scenario)) {
      model <- stats::lm(price ~ year, data = data[data$year >= 2049 & data$year <= 2069 & data$technology == i & data$scenario == j, ])
      data$price[data$technology == i & data$scenario == j] <- ifelse(is.na(data$price[data$technology == i & data$scenario == j]), model$coefficients[2] * data$year[data$technology == i & data$scenario == j] + model$coefficients[1], data$price[data$technology == i & data$scenario == j])
    }
  }
  return(data)
}


#' Create automotive prices
#'
#' @description
#' Synthetic creation of automotive prices by setting them to 1,
#' where automotive sectors exist in the scenario data
#'
#'
#' @param Scenarios_AnalysisInput Scenarios_AnalysisInput
#'
#' @return the automotive price dataframe
#' @export
#'
create_automotive_prices <- function(Scenarios_AnalysisInput) {
  automotive_scenario_data <- Scenarios_AnalysisInput %>%
    dplyr::filter(.data$ald_sector == "Automotive") %>%
    dplyr::distinct(.data$scenario,
                    .data$ald_sector,
                    .data$ald_business_unit,
                    .data$year) %>%
    dplyr::rename(technology = .data$ald_business_unit,
                  sector = .data$ald_sector)

  auto_prices <- automotive_scenario_data %>%
    dplyr::mutate(price = 1,
                  scenario_geography = "Global",
                  indicator="price",
                  unit="dummy")

  return(auto_prices)
}


