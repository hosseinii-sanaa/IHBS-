#' Get candidate tables for income data
#'
#' This internal function returns a vector of candidate table names for income data.
#'
#' @return A character vector of table names.
#'
#' @keywords internal
#' @noRd
.get_income_candidate_tables <- function() {
  c(
    "old_agricultural_self_employed_income",
    "old_non_agricultural_self_employed_income",
    "old_other_income",
    "old_rural_private_employment_income",
    "old_rural_public_employment_income",
    "old_urban_private_employment_income",
    "old_urban_public_employment_income",
    "other_income",
    "self_employed_income",
    "employment_income",
    "subsidy"
  )
}
#' Preload available income tables for a given year
#'
#' This function preloads missing tables for income data by checking the cache and downloading if necessary.
#'
#' @param year The year of the data.
#' @param ... Additional arguments passed to load_table.
#'
#' @return Invisible NULL; side effect is caching tables.
#'
#' @export
preload_income_available_tables <- function(year, ...) {
  candidates <- .get_income_candidate_tables()
  message(" Preloading income tables for year ", year)
  for (tbl in candidates) {
    cache_path <- .get_cache_path(tbl, year)
    if (file.exists(cache_path)) {
      message(" Already cached: ", tbl)
    } else {
      tryCatch({
        load_table(tbl, year, ...)
        if (file.exists(cache_path)) {
          message(" Cached: ", tbl)
        } else {
          message(" Still missing after download: ", tbl)
        }
      }, error = function(e) {
        message(" Not found or failed to download: ", tbl)
      })
    }
  }
}
#' Get income column mapping for known tables
#'
#' This internal function returns the income column name for a given table.
#'
#' @param tbl The table name.
#'
#' @return The column name or NULL.
#'
#' @keywords internal
#' @noRd
.get_income_col <- function(tbl) {
  mapping <- list(
    "employment_income" = "Annual_Net_Income",
    "self_employed_income" = "Profit",
    "subsidy" = "Subsidy",
    "other_income" = NULL, # Special case: sum multiple columns
    "old_agricultural_self_employed_income" = "Profit",
    "old_non_agricultural_self_employed_income" = "Profit",
    "old_other_income" = NULL, # Special case: sum multiple columns
    "old_rural_private_employment_income" = "Annual_Net_Income",
    "old_rural_public_employment_income" = "Annual_Net_Income",
    "old_urban_private_employment_income" = "Annual_Net_Income",
    "old_urban_public_employment_income" = "Annual_Net_Income"
  )
  if (tbl %in% names(mapping)) {
    return(mapping[[tbl]])
  } else {
    return(NULL)
  }
}
#' Rename or compute the income-related column
#'
#' This internal helper function renames or computes the income column in a data frame.
#'
#' @param df The data frame.
#' @param tbl The table name.
#'
#' @return The modified data frame or NULL if failed.
#'
#' @keywords internal
#' @noRd
#'
#' @importFrom dplyr mutate select all_of
rename_income_column <- function(df, tbl) {
  if (tbl %in% c("other_income", "old_other_income")) {
  # Special case for other_income: sum specific income columns
    income_cols <- c("Retirement", "Rent", "Interest", "Aid", "Home_Production", "Transfer")
    missing_cols <- setdiff(income_cols, names(df))
    if (length(missing_cols) > 0) {
      message(paste(" Missing income columns in", tbl, ":", paste(missing_cols, collapse = ", ")))
      return(NULL)
    }
    message(paste(" Computing 'Income' in", tbl, "as sum of", paste(income_cols, collapse = ", ")))
    df <- df %>%
      dplyr::mutate(Income = rowSums(dplyr::select(., dplyr::all_of(income_cols)), na.rm = TRUE))
    return(df)
  }
  original_col <- .get_income_col(tbl)
  if (!is.null(original_col)) {
    if (original_col %in% names(df)) {
      message(paste(" Renaming income column in", tbl, "from", original_col, "to 'Income'"))
      names(df)[names(df) == original_col] <- "Income"
      return(df)
    } else {
      message(paste(" Specified income column", original_col, "not found in", tbl, ". Falling back to search."))
    }
  }
  # Fallback to search if no mapping or column not found
  col_names <- tolower(names(df))
  income_keywords <- c("income", "profit", "subsidy", "value", "amount")
  income_col_idx <- NA
  for (kw in income_keywords) {
    matches <- grep(kw, col_names)
    if (length(matches) == 1) { # Only if exactly one match to avoid multiple
      income_col_idx <- matches[1]
      break
    } else if (length(matches) > 1) {
      message(paste(" Multiple potential income columns found in", tbl, ":", paste(names(df)[matches], collapse = ", "), ". Skipping fallback."))
      return(NULL)
    }
  }
  if (!is.na(income_col_idx)) {
    original_col <- names(df)[income_col_idx]
    message(paste(" Renaming income column in", tbl, "from", original_col, "to 'Income' (fallback)"))
    names(df)[income_col_idx] <- "Income"
    return(df)
  } else {
    message(paste(" No income-related column found in", tbl, ". Available columns:", paste(names(df), collapse = ", ")))
    return(NULL) # Skip this table if no income column found
  }
}
#' Load income data for a given year
#'
#' This function loads all cached income tables for a given year, with options to combine and summarize.
#'
#' @param year The year of the data.
#' @param combine Logical. If TRUE, combine all tables.
#' @param summarise Logical. If TRUE, summarize the combined data.
#' @param verbose Logical. If TRUE, print messages.
#' @param ... Additional arguments passed to load_table.
#'
#' @return A data.frame or list of data.frames, depending on parameters.
#'
#' @export
#'
#' @importFrom dplyr bind_rows group_by summarise mutate
load.income <- function(year, combine = TRUE, summarise = TRUE, verbose = TRUE, ...) {
  app_name <- "rhbsirdata"
  cache_dir <- rappdirs::user_cache_dir(appname = app_name)
  pattern <- paste0("^", year, "_.*\\.parquet$")
  files <- list.files(cache_dir, pattern = pattern, full.names = TRUE)
  #Get the table names from the file names
  table_names <- basename(files)
  table_names <- sub(paste0("^", year, "_"), "", table_names)
  table_names <- sub("\\.parquet$", "", table_names)
  #Filter only candidate tables
  candidate_tables <- .get_income_candidate_tables()
  table_names <- table_names[table_names %in% candidate_tables]
  if (length(table_names) == 0) {
    message(" No cached income tables found  attempting preload...")
    preload_income_available_tables(year, ...)
    files <- list.files(cache_dir, pattern = pattern, full.names = TRUE)
    table_names <- basename(files)
    table_names <- sub(paste0("^", year, "_"), "", table_names)
    table_names <- sub("\\.parquet$", "", table_names)
    table_names <- table_names[table_names %in% candidate_tables]
  }
  if (length(table_names) == 0) {
    stop(paste(" No cached income tables found for year", year, "even after attempting preload."))
  }
  if (verbose) {
    message(" Using cached income tables: ", paste(table_names, collapse = ", "))
  }
  all_data <- lapply(table_names, function(tbl) {
    tryCatch({
      df <- load_table(tbl, year, ...)
      #Rename or compute the income column
      df <- rename_income_column(df, tbl)
      if (is.null(df)) {
        return(NULL)
      }
      df
    }, error = function(e) {
      message(" Error loading or processing: ", tbl)
      return(NULL)
    })
  })
  all_data <- Filter(Negate(is.null), all_data)
  if (length(all_data) == 0) {
    stop(" All income table loads failed  no valid data found.")
  }
  if (combine) {
    id_col <- "ID"
    if (!all(sapply(all_data, function(df) id_col %in% names(df)))) {
      stop(" Not all income tables contain the expected ID column: 'ID'")
    }
    for (i in seq_along(all_data)) {
      if (!"name" %in% names(all_data[[i]])) {
        all_data[[i]]$name <- table_names[i]
      }
    }
    df_combined <- dplyr::bind_rows(all_data)
    if (summarise) {
      if (!"Income" %in% names(df_combined)) {
        stop(" Standardized 'Income' column not found in combined data after renaming.")
      }
      df_summarized <- df_combined %>%
        dplyr::mutate(ID = as.character(ID)) %>%
        dplyr::group_by(year, ID) %>%
        dplyr::summarise(Gross_Income = sum(Income, na.rm = TRUE), .groups = "drop")
      return(df_summarized)
    } else {
      return(df_combined)
    }
  } else {
    return(all_data)
  }
}
