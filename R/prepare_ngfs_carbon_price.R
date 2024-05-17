#' This function reads carbon price data sourced from the NGFS scenario selector
#' tool and prepares it for use in the trisk.model package. Gaps
#' between years are interpolated.
#'
#' @param data Tibble that contains the raw ngfs scenario carbon price data file
#'   that is to be processed
#' @param start_year Numeric vector of length 1. Indicates the desired start
#'   year of the prepared ngfs price data set
#'
#' @family data preparation functions

prepare_ngfs_carbon_price <- function(data,
                                      start_year) {
  # for now end year is set to 2100. Think about surfacing this if necessary
  end_year <- 2100

  data_has_expected_columns <- all(
    c(
      "Model", "Scenario", "Region", "Variable", "Unit", as.character(end_year)
    ) %in% colnames(data)
  )
  stopifnot(data_has_expected_columns)

  # carbon tax is later interpolated on a yearly basis. This check ensures the
  # first year is outside of the interpolation range
  start_year_in_bounds <- any(
    c((start_year - 4):start_year) %in% colnames(data)
  )
  stopifnot(start_year_in_bounds)

  data <- data %>%
    dplyr::rename(
      model = .data$Model,
      scenario = .data$Scenario,
      scenario_geography = .data$Region,
      variable = .data$Variable,
      unit = .data$Unit
    ) %>%
    dplyr::mutate(
      scenario_geography = dplyr::if_else(
        .data$scenario_geography == "World",
        "Global",
        .data$scenario_geography
      )
    )

  ## I add here a no carbon tax model that serves as the default in the stress test
  no_carbon_tax <- tibble::tribble(
    ~model, ~scenario, ~scenario_geography, ~variable, ~unit, ~`2015`, ~`2020`, ~`2025`, ~`2030`, ~`2035`, ~`2040`, ~`2045`, ~`2050`,
    ~`2055`, ~`2060`, ~`2065`, ~`2070`, ~`2075`, ~`2080`, ~`2085`, ~`2090`, ~`2095`, ~`2100`,
    "no_carbon_tax", "no_carbon_tax", "Global", "Price|Carbon", "US$2010/t CO2", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
  )
  
  flat_carbon_tax_50 <- tibble::tribble(
    ~model, ~scenario, ~scenario_geography, ~variable, ~unit, ~`2015`, ~`2020`, ~`2025`, ~`2030`, ~`2035`, ~`2040`, ~`2045`, ~`2050`,
    ~`2055`, ~`2060`, ~`2065`, ~`2070`, ~`2075`, ~`2080`, ~`2085`, ~`2090`, ~`2095`, ~`2100`,
    "flat_carbon_tax_50", "flat_carbon_tax_50", "Global", "Price|Carbon", "US$2010/t CO2", 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50
  )
  
  increasing_carbon_tax_50 <- tibble::tribble(
    ~model, ~scenario, ~scenario_geography, ~variable, ~unit, ~`2015`, ~`2020`, ~`2025`, ~`2030`, ~`2035`, ~`2040`, ~`2045`, ~`2050`,
    ~`2055`, ~`2060`, ~`2065`, ~`2070`, ~`2075`, ~`2080`, ~`2085`, ~`2090`, ~`2095`, ~`2100`,
    "increasing_carbon_tax_50", "increasing_carbon_tax_50", "Global", "Price|Carbon", "US$2010/t CO2", 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50
  )
  
  independent_increasing_carbon_tax_50 <- tibble::tribble(
    ~model, ~scenario, ~scenario_geography, ~variable, ~unit, ~`2015`, ~`2020`, ~`2025`, ~`2030`, ~`2035`, ~`2040`, ~`2045`, ~`2050`,
    ~`2055`, ~`2060`, ~`2065`, ~`2070`, ~`2075`, ~`2080`, ~`2085`, ~`2090`, ~`2095`, ~`2100`,
    "independent_increasing_carbon_tax_50", "independent_increasing_carbon_tax_50", "Global", "Price|Carbon", "US$2010/t CO2", 0, 0, 50, 60.83, 74.01, 90.05, 109.56, 133.29, 162.17, 197.30, 240.05, 292.06, 355.33, 432.32, 525.98, 639.94, 778.58, 947.26
  )

  data <- data %>% rbind(no_carbon_tax)
  
  data <- data %>% rbind(flat_carbon_tax_50)
  data <- data %>% rbind(increasing_carbon_tax_50)
  data <- data %>% rbind(independent_increasing_carbon_tax_50)

  data$`2025` <- ifelse(data$scenario == "NDC_Indonesia_moderate", 2, data$`2025`)
  data$`2030` <- ifelse(data$scenario == "NDC_Indonesia_moderate", NA, data$`2030`)
  data$`2035` <- ifelse(data$scenario == "NDC_Indonesia_moderate", 10, data$`2035`)
  data$`2040` <- ifelse(data$scenario == "NDC_Indonesia_moderate", NA, data$`2040`)
  data$`2045` <- ifelse(data$scenario == "NDC_Indonesia_moderate", NA, data$`2045`)
  
  
  
  data$`2025` <- ifelse(data$scenario == "NDC_Indonesia_market_assumption", 5, data$`2025`)
  data$`2030` <- ifelse(data$scenario == "NDC_Indonesia_market_assumption", NA, data$`2030`)
  data$`2035` <- ifelse(data$scenario == "NDC_Indonesia_market_assumption", 35, data$`2035`)
  
  data$`2025` <- ifelse(data$scenario == "NZ2050_Indonesia_market_assumption", 5, data$`2025`)
  data$`2030` <- ifelse(data$scenario == "NZ2050_Indonesia_market_assumption", NA, data$`2030`)
  data$`2035` <- ifelse(data$scenario == "NZ2050_Indonesia_market_assumption", 35, data$`2035`)

  data <- data %>%
    tidyr::pivot_longer(
      cols = tidyr::starts_with(c("20", "21")),
      names_to = "year",
      values_to = "carbon_tax"
    ) %>%
    dplyr::mutate(year = as.numeric(.data$year))

  data <- data %>%
    tidyr::complete(
      year = seq(min(.data$year), .env$end_year),
      tidyr::nesting(
        !!!rlang::syms(
          c("model", "scenario", "scenario_geography", "variable", "unit")
        )
      )
    ) %>%
    dplyr::arrange(
      .data$model, .data$scenario, .data$scenario_geography, .data$variable,
      .data$unit, .data$year
    ) %>%
    dplyr::group_by(
      .data$model, .data$scenario, .data$scenario_geography, .data$variable,
      .data$unit
    ) %>%
    dplyr::mutate(
      carbon_tax = dplyr::case_when(
        .data$scenario == "DN0" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NDC" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NZ2050" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "B2DS" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "DN0_Indonesia" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "B2DS_Indonesia" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NZ2050_Indonesia" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NDC_Indonesia_moderate" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NDC_Indonesia_market_assumption" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "NZ2050_Indonesia_market_assumption" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "flat_carbon_tax_50" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "independent_increasing_carbon_tax_50" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "increasing_carbon_tax_50" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        .data$scenario == "DT_Indonesia" &
          .data$year >= 2025 ~
          zoo::na.approx(object = .data$carbon_tax),
        TRUE ~ 0
      )
    ) %>%
    dplyr::ungroup()

  output_has_expected_columns <- all(
    c(
      "year", "model", "scenario", "scenario_geography", "variable", "unit",
      "carbon_tax"
    ) %in% colnames(data)
  )
  stopifnot(output_has_expected_columns)

  return(data)
}
