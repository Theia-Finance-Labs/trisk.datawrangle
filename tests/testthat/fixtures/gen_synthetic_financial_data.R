
box::use(
  fixtures/ synthetic_file_paths[synth_fp],
)

library(trisk.datawrangle)

get_path_to_root_from_test
company_activities <- arrow::read_parquet(synth_fp$company_activities)
eikon_data <- arrow::read_parquet(synth_fp$eikon_data)


companies_data <- company_activities |> dplyr::distinct(company_id, ald_sector, ald_location)

prewrangled_financial_data_stress_test <- prepare_financial_data(
  financial_data = eikon_data,
  companies_data = companies_data,
  ownership_tree = NULL,
  minimum_sample_size = 1,
  minimum_ratio_sample = 0,
  allowed_range_npm = c(-Inf, Inf)
)

abcd_stress_test_input <- arrow::read_parquet(synth_fp$abcd_stress_test_input)

prewrangled_financial_data_stress_test <- prewrangled_financial_data_stress_test %>%
  dplyr::inner_join(abcd_stress_test_input %>% dplyr::distinct(company_id))



arrow::write_parquet(prewrangled_financial_data_stress_test, synth_fp$prewrangled_financial_data_stress_test)
