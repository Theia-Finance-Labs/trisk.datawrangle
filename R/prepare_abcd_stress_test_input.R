#' Sum values by skipping nans, or return nan if all values are nan
#' (instead of 0 with sum(... na.rm=T))
#'
#' @param array a vector of numbers
#'
#' @return sum of array or nan
#'
.sum_or_all_nans <- function(array) {
  if (all(is.na(array))) {
    return(NA)
  } else {
    return(sum(array, na.rm = TRUE))
  }
}


#' pivot values of Equity Ownership, to be used as yearly production/emissions
#' TODO when Direct Ownership is not nan, use it when Equity Ownership is nan ?
#'
#' @param ar_data ar_data
#'
pivot_equity_ownership_columns <- function(ar_data) {
  ar_data <- ar_data %>%
    tidyr::pivot_longer(
      cols = dplyr::starts_with("Equity Ownership "),
      names_to = "year",
      values_to = "equity_ownership"
    ) %>%
    dplyr::mutate(year = stringr::str_extract(.data$year, stringr::regex("\\d+")))

  return(ar_data)
}


#' aggregate volumes of equity_ownership over duplicated rows
#'
#' @param ar_data ar_data
#'
aggregate_equity_ownership <- function(ar_data) {
  ar_data <- ar_data %>%
    dplyr::group_by(dplyr::across(c(-.data$equity_ownership))) %>%
    dplyr::summarise(equity_ownership = .sum_or_all_nans(.data$equity_ownership)) %>%
    dplyr::ungroup()

  return(ar_data)
}



#' Merge production and emissions data.
#' Filter rows where the production unit and emission unit match as expected
#' @param company_activities company_activities dataframe
#' @param company_emissions company_emissions dataframe
#'
match_emissions_to_production <- function(company_activities,
                                          company_emissions) {
  company_activities <- company_activities %>%
    dplyr::rename(
      ald_production_unit = .data$activity_unit,
      ald_production = .data$equity_ownership
    )
  company_emissions <- company_emissions %>%
    dplyr::rename(
      emissions_factor_unit = .data$activity_unit,
      emissions_factor = .data$equity_ownership
    )

  abcd_data <-
    dplyr::full_join(
      company_activities,
      company_emissions,
      by = c(
        "company_id",
        "company_name",
        "ald_sector",
        "ald_business_unit",
        "ald_location",
        "year"
      )
    )
  
  return(abcd_data)
}

#' Create new rows replicating the nesting columns for each year of the desired
#'  scope not present.
#' New rows have NA values for columns outside nesting.
#'
#' @param abcd_data abcd_data
#' @param start_year start_year
#' @param time_horizon time_horizon
#'
#' @return abcd_data
#'
create_missing_year_rows <- function(abcd_data, start_year, time_horizon) {
  abcd_data <- abcd_data %>%
    dplyr::mutate(year = as.numeric(.data$year)) %>%
    tidyr::complete(
      year = seq(start_year, start_year + time_horizon),
      tidyr::nesting(!!!rlang::syms(c(
        "company_id", "company_name", "ald_sector", "ald_business_unit",
        "ald_location", "ald_production_unit", "emissions_factor_unit"
      )))
    )
  return(abcd_data)
}

#' filter to keep only desired years
#' Filtering years and fill/create missing years is done independantly, because
#' other cleaning steps
#'
#' @param abcd_data abcd_data
#' @param start_year start_year
#' @param time_horizon time_horizon
#' @param additional_year additional_year
#'
filter_years_abcd_data <- function(abcd_data,
                                   start_year,
                                   time_horizon,
                                   additional_year) {
  abcd_data <- abcd_data %>%
    dplyr::filter(.data$year %in% c(start_year:(start_year + time_horizon), additional_year))
  return(abcd_data)
}

#' use avg EFs per ald_business_unit to fill missing values
#'
#' @param abcd_data abcd_data
#'
fill_missing_emission_factor <- function(abcd_data) {
  # compute average emissions_factor
  avg_emission_factors <- abcd_data %>%
    dplyr::group_by(
      .data$ald_sector,
      .data$ald_business_unit,
      .data$emissions_factor_unit
    ) %>%
    dplyr::summarise(emissions_factor = mean(.data$emissions_factor, na.rm = T)) %>%
    dplyr::ungroup()
  
  # fill missing emission factors with averages
  abcd_missing_ef <- abcd_data %>%
    dplyr::filter(is.na(.data$emissions_factor))

  abcd_missing_ef <- abcd_missing_ef %>%
    dplyr::select(-.data$emissions_factor, -.data$emissions_factor_unit) %>%
    dplyr::left_join(avg_emission_factors,
      by = c("ald_sector", "ald_business_unit")
    )

  # Fill nans when there is no avg emission factor for some technologies
  # TODO why is there no emission factor on HDV ?
  abcd_missing_ef <-
    abcd_missing_ef %>%
    dplyr::mutate(emissions_factor = tidyr::replace_na(.data$emissions_factor, 0))

  abcd_data <- abcd_data %>%
    dplyr::filter(!is.na(.data$emissions_factor)) %>%
    dplyr::bind_rows(abcd_missing_ef)

  # Set the emission factor to 0 when production is 0
  abcd_data  <- abcd_data %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      emissions_factor = dplyr::if_else(.data$ald_production == 0, 0, .data$emissions_factor)
    )

  return(abcd_data)
}

#' convert EF from tCO2 (or tCO2e) to the ratio of tCO2 (or tCO2e) over production
#'
#' @param abcd_data abcd_data
#'
create_emissions_factor_ratio <- function(abcd_data) {
  
  abcd_data <-
    abcd_data %>% dplyr::mutate(
      emissions_factor = .data$emissions_factor / .data$ald_production,
      emissions_factor_unit = dplyr::if_else(
        !is.na(.data$emissions_factor_unit),
        paste(
          .data$emissions_factor_unit,
          .data$ald_production_unit,
          sep = "/"
        ),
        "unknown" # unkown emissions_factor_unit
      )
    )

  # match the MW production to tCO2/MWh emissions
  # TODO Why forcing the use of MW for prod and tCO2/MWh while we can do all MW or all MWh ?
  abcd_MW_prod <- abcd_data %>%
    dplyr::filter(.data$ald_production_unit == "MW") %>%
    dplyr::select(-.data$emissions_factor_unit, -.data$emissions_factor)%>% 
    dplyr::distinct_all()
  abcd_MWh_emissions <- abcd_data %>%
    dplyr::filter(.data$ald_production_unit == "MWh") %>%
    dplyr::select(-.data$ald_production_unit, -.data$ald_production)%>% 
    dplyr::distinct_all()
  abcd_MW_prod_MWh_emissions <-
    dplyr::inner_join(abcd_MW_prod, abcd_MWh_emissions)
  abcd_data <- dplyr::bind_rows(
    abcd_data %>%
      dplyr::filter(!.data$ald_production_unit %in% c("MW", "MWh")),
    abcd_MW_prod_MWh_emissions
  )

  return(abcd_data)
}


#' Drop rows where production is always NA or 0 over the time period
#' @param abcd_data abcd_data
#'
drop_always_empty_production <- function(abcd_data) {
  empty_on_all_years <- abcd_data %>%
    dplyr::group_by(dplyr::across(
      c(
        -.data$year,
        -.data$ald_production,
        -.data$emissions_factor
      )
    )) %>%
    dplyr::summarise(
      all_nans_prod = all(is.na(.data$ald_production)) | (sum(.data$ald_production) == 0)
    ) %>%
    dplyr::ungroup()

  rows_to_drop <- empty_on_all_years %>%
    dplyr::filter(.data$all_nans_prod == TRUE) %>%
    dplyr::select(c(-.data$all_nans_prod))

  abcd_data <- abcd_data %>% dplyr::anti_join(rows_to_drop)

  return(abcd_data)
}

#' Sum productions and emissions over all other columns.
#' This is done to aggregate those metrics over the different
#' countries in a region where a company operates in.
#'
#' @param abcd_data abcd_data
#'
#' @return abcd_data
aggregate_over_locations <- function(abcd_data) {
  abcd_data <- abcd_data %>%
    dplyr::group_by(dplyr::across(
      c(
        -.data$ald_production, -.data$emissions_factor
      )
    )) %>%
    dplyr::summarise(
      ald_production = .sum_or_all_nans(.data$ald_production),
      emissions_factor = .sum_or_all_nans(.data$emissions_factor)
    ) %>%
    dplyr::ungroup()
  return(abcd_data)
}

#' Fill ald_production and emissions_factor for a given ald_business_unit at a company
#'  with values of previous years, or with interpolation for values in the middle, or 0 for values in previous years.
#'
#' @param abcd_data abcd_data
#'
#'
fill_partially_missing_values <- function(abcd_data) {
  abcd_data <- abcd_data %>%
    dplyr::group_by(
      .data$company_id,
      .data$ald_sector,
      .data$ald_business_unit,
      .data$ald_location,
      .data$emissions_factor_unit,
      .data$ald_production_unit
    ) %>%
    dplyr::arrange(
      .data$year,
      .by_group = T
    ) %>%
    dplyr::mutate(
      # Fill years in the middle with interpolation
      ald_production = zoo::na.approx(.data$ald_production, na.rm = F),
      emissions_factor = zoo::na.approx(.data$emissions_factor, na.rm = F),
    ) %>%
    # Fill years in the beginning and et the end, by extending the first or last non-na value
    tidyr::fill(.data$ald_production, .direction = "down") %>%
    tidyr::fill(.data$emissions_factor, .direction = "down") %>%
    dplyr::ungroup()  %>%
    dplyr::mutate(
      ald_production=tidyr::replace_na(.data$ald_production, 0) ,
     emissions_factor= tidyr::replace_na(.data$emissions_factor, 0)
    )
  return(abcd_data)
}



#' Filter dataframe to keep only desired sectors
#' @param abcd_data abcd_data
#' @param sector_list list of sectors to keep
#'
filter_sectors_abcd_data <- function(abcd_data, sector_list) {
  abcd_data <- abcd_data %>%
    dplyr::filter(.data$ald_sector %in% sector_list)
  return(abcd_data)
}

create_asset_id <- function(abcd_data) {
  abcd_data_asset_ids <- abcd_data %>%
    dplyr::distinct(.data$company_id, .data$ald_location, .data$ald_business_unit) %>%
    dplyr::group_by(.data$company_id) %>%
    dplyr::mutate(asset_id = paste0(.data$company_id, "_", dplyr::row_number())) %>%
    dplyr::ungroup()

  abcd_data  <- abcd_data %>%
    dplyr::right_join(abcd_data_asset_ids, by=c("company_id", "ald_location", "ald_business_unit"))
  
  return(abcd_data)
}


#' Creates the abcd stress test input data, using companies emissions and productions.
#' Aggregate absolute values of production and emissions over ald_business_unit types and regions.
#' Computes the emissions factor ratio out of absolute emissions and productions.
#'
#' @param company_activities production of companies in different countries.
#'    Production amount is indicated in equity_ownership columns, 1 column per year
#' @param company_emissions emissions of companies in different countries.
#'    Emission amount is indicated in equity_ownership columns, 1 column per year
#' @param scenarios_geographies mapping between country code and climate scenario geographies
#' @param start_year start_year
#' @param time_horizon time_horizon
#' @param additional_year list of years to add to the year range
#' @param sector_list sector_list
#'
#' @return companies production matched to the appropriate emission, 1 row per year and scenario geography
#' @export
prepare_abcd_data <- function(company_activities,
                              company_emissions,
                              scenarios_geographies,
                              start_year,
                              time_horizon,
                              additional_year,
                              sector_list) {
  # TODO CHECK THAT IF A PRODUCTION IS IN MW IT IS ALSO IN MWh
  
  company_activities <-
    pivot_equity_ownership_columns(company_activities)
  company_emissions <-
    pivot_equity_ownership_columns(company_emissions)

  ## AGGREGATIONS
  company_activities <-
    aggregate_equity_ownership(company_activities)
  company_emissions <-
    aggregate_equity_ownership(company_emissions)

  # From here, shape of activities and emissions data :
  #   company_activities : production of companies, 1 row per year
  #   company_emissions : emissions of companies (in tCO2 or tCO2e), 1 row per year

  ###### ABCD

  ## DATALOAD
  abcd_data <-
    match_emissions_to_production(company_activities, company_emissions) 

    abcd_data <- abcd_data %>%
      dplyr::filter(!is.na(.data$ald_location)) %>%
      aggregate_over_locations()

  abcd_data <- create_missing_year_rows(abcd_data, start_year, time_horizon)


  ## AGGREGATIONS

  abcd_data <- fill_partially_missing_values(abcd_data)

  # at this point, nans in ald_production are only due to fully empty production in raw data
  # to check that, only 2 values with this command:
  #   abcd_data %>% group_by(company_id, company_name, ald_location, ald_sector, ald_business_unit, ald_production_unit, emissions_factor_unit) %>% summarise(nna=sum(is.na(ald_production))) %>% ungroup() %>% distinct(nna)

  abcd_data <- create_emissions_factor_ratio(abcd_data) # TODO rework/remove this function
  abcd_data <- fill_missing_emission_factor(abcd_data)

  # nans in emission_factor only on all years of a given thech (same as above)
  # to check :
  #  abcd_data %>% group_by(company_id, company_name, ald_location, ald_sector, ald_business_unit, ald_production_unit, emissions_factor_unit) %>% summarise(nna=sum(is.na(emissions_factor))) %>% ungroup() %>% distinct(nna)

    # abcd_data %>%
    #   assertr::verify(all(colSums(is.na(.)) == 0)) 
      # assertr::assert(nrow(.) == nrow(. %>% dplyr::distinct_all()))

  abcd_data <- drop_always_empty_production(abcd_data)

  abcd_data <- abcd_data %>%
    dplyr::rename(
      plan_tech_prod = .data$ald_production,
      plan_emission_factor = .data$emissions_factor
    )

  ## FILTERINGS
  abcd_data <-
    filter_sectors_abcd_data(abcd_data, sector_list = sector_list)
  abcd_data <-
    filter_years_abcd_data(
      abcd_data,
      start_year = start_year,
      time_horizon = time_horizon,
      additional_year = additional_year
    )

  abcd_data <- abcd_data %>%
    create_asset_id()%>%
    dplyr::mutate(
      scenario_geography="Global"
    )%>%
    dplyr::rename(
      country_iso2=.data$ald_location
    )
    
  stopifnot(nrow(abcd_data) == nrow(abcd_data %>% dplyr::distinct_all()))

  return(abcd_data)
}
