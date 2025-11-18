#' Get the local cache path for a specific HBSIR data file
#'
#' This internal helper function determines the full path for storing or
#' retrieving a cached HBSIR data file. It uses `rappdirs` to find an
#' OS-appropriate cache directory and creates a subdirectory for this package.
#'
#' @param table_name Character string. The specific name of the HBSIR table
#'   (e.g., "food", "income").
#' @param year Numeric or character string. The year of the HBSIR data.
#'
#' @return A character string representing the full, absolute path to where the
#'   cached Parquet file should be located.
#'
#' @keywords internal
#' @noRd
.get_cache_path <- function(table_name, year) {
  stopifnot(!missing(table_name), !missing(year))
  cache_base <- rappdirs::user_cache_dir(appname = "rhbsirdata")
  dir.create(cache_base, recursive = TRUE, showWarnings = FALSE)
  file_name <- paste0(year, "_", table_name, ".parquet")
  full_path <- file.path(cache_base, file_name)
  return(normalizePath(full_path, winslash = "/", mustWork = FALSE))
}

#' Download a file from S3 to the local cache
#'
#' This internal helper function handles the actual download of a Parquet file
#' from a given S3 URL to a specified local destination path. It includes
#' error handling for the download process.
#'
#' @param s3_url Character string. The full URL of the Parquet file on S3.
#' @param destination_path Character string. The full local file path where the
#'   downloaded file should be saved.
#'
#' @return Invisibly returns `TRUE` if the download is successful. Stops with
#'   an error message if the download fails.
#'
#' @keywords internal
#' @noRd
.download_to_cache <- function(s3_url, destination_path) {
  message("Downloading data from: ", s3_url)
  tryCatch({
    utils::download.file(url = s3_url, destfile = destination_path,
                         mode = "wb", quiet = TRUE)
  }, error = function(e) {
    stop(paste0(
      "Failed to download the file from '", s3_url, "'.\n",
      "Please check your internet connection and ensure the URL is correct.\n",
      "Original error: ", e$message
    ))
  })
  invisible(TRUE)
}
