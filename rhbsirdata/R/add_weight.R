#' Add household weights to a data frame
#'
#' This function adds household sampling weights to a given data frame using either
#' household_information tables (if available) or an external fallback file.
#'
#' @param df A data frame that must include 'year' and 'ID' columns.
#' @return The same data frame with an added 'Weight' column.
hbsir.add_weight <- function(df) {
  stopifnot(all(c("year", "ID") %in% names(df)))
  df <- dplyr::mutate(df, ID = as.character(ID))
  years <- unique(df$year)

  # Function to download and read fallback weights
  get_fallback_weights <- function() {
    fallback_url <- "https://s3.ir-tbz-sh1.arvanstorage.ir/iran-open-data/EXTERNAL/hbsir_weights.parquet"
    fallback_file <- tempfile(fileext = ".parquet")
    message(" Downloading fallback weights from external source...")

    tryCatch({
      utils::download.file(fallback_url, fallback_file, mode = "wb", quiet = TRUE)
      arrow::read_parquet(fallback_file) |>
        dplyr::rename_with(tolower) |>
        dplyr::mutate(ID = as.character(id)) |>
        dplyr::select(year, ID, Weight = dplyr::matches("(?i)weight"))
    }, error = function(e) {
      stop(" Failed to load fallback weights from external source.")
    })
  }

  fallback_weights <- NULL

  # Build weights table by year
  weights <- purrr::map_dfr(years, function(yr) {
    wt <- tryCatch({
      hh <- load_table("household_information", yr)
      weight_col <- grep("(?i)weight", names(hh), value = TRUE)

      if (length(weight_col) == 0) {
        warning(glue::glue(" 'household_information' for year {yr} has no weight column. Using fallback."))
        if (is.null(fallback_weights)) fallback_weights <<- get_fallback_weights()
        return(fallback_weights |> dplyr::filter(year == yr))
      }

      hh |>
        dplyr::mutate(
          ID = as.character(ID),
          year = yr
        ) |>
        dplyr::select(year, ID, Weight = all_of(weight_col[1]))

    }, error = function(e) {
      message(glue::glue(" Household info missing for year {yr}, using fallback weights."))
      if (is.null(fallback_weights)) fallback_weights <<- get_fallback_weights()
      fallback_weights |> dplyr::filter(year == yr)
    })

    return(wt)
  })

  # Join weights to original df
  dplyr::left_join(df, weights, by = c("year", "ID"))
}
