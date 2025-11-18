#' Add Percentile, Decile, and Quantile to HBSIR Data
#'
#' This function adds percentile, decile, and quantile (using quartiles as an example) calculations
#' to a data frame containing HBSIR data, based on a specified income or expenditure column.
#'
#' @param df A data frame containing the HBSIR data with a column to rank (e.g., "Gross_Income" or "Gross_Expenditure").
#' @param column The name of the column to calculate ranks on (e.g., "Gross_Income").
#' @return A data frame with added columns: `<column>_Percentile`, `<column>_Decile`, and `<column>_Quartile`.
#' @export
hbsir_add_percentile_decile_quantile <- function(df,column) {
  if (!column %in% names(df)) {
    stop(paste("Column", column, "not found in the data frame."))
  }

  df %>%
    dplyr::mutate(
      !!paste0(column, "_Percentile") := dplyr::percent_rank(.data[[column]]) * 100,
      !!paste0(column, "_Decile") := dplyr::ntile(.data[[column]], 10),
      !!paste0(column, "_Quartile") := dplyr::ntile(.data[[column]], 4)
    )
}
