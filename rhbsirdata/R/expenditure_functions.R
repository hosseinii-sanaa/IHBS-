#' Get candidate tables for expenditure data
#'
#' This internal function returns a vector of candidate table names for expenditure data.
#'
#' @return A character vector of table names.
#'
#' @keywords internal
#' @noRd
.get_candidate_tables <- function() {
  c(
    "cloth", "durable", "entertainment", "food", "furniture", "home", "medical",
    "miscellaneous", "transportation", "investment", "communication", "education", "hotel", "tobacco"
  )
}

#' Preload available tables for a given year
#'
#' This function preloads missing tables for expenditure data by checking the cache and downloading if necessary.
#'
#' @param year The year of the data.
#' @param ... Additional arguments passed to load_table.
#'
#' @return Invisible NULL; side effect is caching tables.
#'
#' @export
preload_available_tables <- function(year, ...) {
  candidates <- .get_candidate_tables()
  message(" Preloading tables for year ", year)

  for (tbl in candidates) {
    cache_path <- .get_cache_path(tbl, year)
    if (file.exists(cache_path)) {
      message(" Already cached= ", tbl)
    } else {
      tryCatch({
        load_table(tbl, year, ...)
        if (file.exists(cache_path)) {
          message(" Cached= ", tbl)
        } else {
          message(" Still missing after download= ", tbl)
        }
      }, error = function(e) {
        message(" Not found or failed to download= ", tbl)
      })
    }
  }
}

#' Load expenditure data for a given year
#'
#' This function loads all cached expenditure tables for a given year, with options to combine and summarize.
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
load.expenditure <- function(year, combine = TRUE, summarise = TRUE, verbose = TRUE, ...) {
  app_name <- "rhbsirdata"
  cache_dir <- rappdirs::user_cache_dir(appname = app_name)
  pattern <- paste0("^", year, "_.*\\.parquet$")

  files <- list.files(cache_dir, pattern = pattern, full.names = TRUE, recursive = TRUE)

  # Get the table names from the file names
  table_names <- basename(files)
  table_names <- sub(paste0("^", year, "_"), "", table_names)
  table_names <- sub("\\.parquet$", "", table_names)

  # Filter only candidate tables
  candidate_tables <- .get_candidate_tables()
  table_names <- table_names[table_names %in% candidate_tables]

  if (length(table_names) == 0) {
    message(" No cached expenditure tables found  attempting preload...")
    preload_available_tables(year, ...)
    files <- list.files(cache_dir, pattern = pattern, full.names = TRUE, recursive = TRUE)
    table_names <- basename(files)
    table_names <- sub(paste0("^", year, "_"), "", table_names)
    table_names <- sub("\\.parquet$", "", table_names)
    table_names <- table_names[table_names %in% candidate_tables]

    # If still no tables after preload, warn and return NULL or empty data frame
    if (length(table_names) == 0) {
      warning(paste(" No expenditure tables found for year", year, "even after attempting preload. Data may not be available for this year."))
      return(data.frame()) # Return empty data frame instead of stopping
    }
  }

  if (verbose) {
    message(" Using cached expenditure tables: ", paste(table_names, collapse = ", "))
  }

  all_data <- lapply(table_names, function(tbl) {
    tryCatch({
      load_table(tbl, year, ...)
    }, error = function(e) {
      message(" Error loading: ", tbl)
      return(NULL)
    })
  })

  all_data <- Filter(Negate(is.null), all_data)

  if (length(all_data) == 0) {
    warning(" All expenditure table loads failed  no valid data found. Returning empty data frame.")
    return(data.frame()) # Return empty data frame instead of stopping
  }

  if (combine) {
    id_col <- "ID"

    if (!all(sapply(all_data, function(df) id_col %in% names(df)))) {
      stop(" Not all expenditure tables contain the expected ID column: 'ID'")
    }

    for (i in seq_along(all_data)) {
      if (!"name" %in% names(all_data[[i]])) {
        all_data[[i]]$name <- table_names[i]
      }
    }

    df_combined <- dplyr::bind_rows(all_data, .id = "source_index")
    df_combined$source_index <- NULL

    if (summarise) {
      if (!"Expenditure" %in% names(df_combined)) {
        stop(" Column 'Expenditure' not found in combined data.")
      }

      df_summarized <- df_combined %>%
        dplyr::mutate(ID = as.character(ID)) %>%
        dplyr::group_by(year, ID) %>%
        dplyr::summarise(Gross_Expenditure = sum(Expenditure, na.rm = TRUE), .groups = "drop")

      return(df_summarized)
    } else {
      return(df_combined)
    }
  } else {
    return(all_data)
  }
}
