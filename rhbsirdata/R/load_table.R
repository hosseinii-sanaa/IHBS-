#' Load HBSIR Data with Local Caching
#'
#' Downloads a specified HBSIR (Household Budget Survey Iran) data table
#' for a given year from a public S3 repository. The function implements
#' local caching to avoid re-downloading data on subsequent requests.
#'
#' The S3 URL structure is assumed to be=
#' `https://s3.ir-tbz-sh1.arvanstorage.ir/iran-open-data/HBSIR/4_cleaned/YEAR_TABLENAME.parquet`
#' For example, for `table_name = "food"` and `year = 1400`, it would attempt to
#' download from `.../4_cleaned/1400_food.parquet`.
#'
#' @param table_name Character string. The name of the HBSIR data table to load
#'   (e.g., "food", "urban_expenditures").
#' @param year Numeric or character string. The year for which the data is
#'   required (e.g., 1400, 1401).
#' @param as_data_frame Logical. If `TRUE` (the default), the function returns a
#'   standard R `data.frame`. If `FALSE`, it returns a more memory-efficient
#'   `arrow::Table` object.
#' @param force_download Logical. If `TRUE`, the function will download the data
#'   from S3 even if a cached version exists locally. Defaults to `FALSE`.
#'
#' @return If `as_data_frame` is `TRUE`, a `data.frame`.
#'         If `as_data_frame` is `FALSE`, an `arrow::Table` object.
#'         Stops with an error if the data cannot be downloaded or read.
#'
#' @source The data is sourced from the Iran Open Data initiative, hosted on
#'   ArvanCloud S3 storage.
#'
#' @export
#'
#' @importFrom arrow read_parquet
#' @importFrom rappdirs user_cache_dir
#' @importFrom utils download.file
#'
#' @examples
#' \dontrun{
#'   # Load the 'food' table for the year 1400 as a data.frame
#'   food_data_1400 <- load_table(table_name = "food", year = 1400)
#'   head(food_data_1400)
#'
#'   # Load the 'income' table for year 1399 as an Arrow Table
#'   income_arrow_1399 <- load_table(table_name = "income", year = 1399, as_data_frame = FALSE)
#'   print(income_arrow_1399)
#'
#'   # Force a re-download of the 'food' table for year 1400
#'   food_data_1400_fresh <- load_table(table_name = "food", year = 1400, force_download = TRUE)
#' }
load_table <- function(table_name,
                       year,
                       as_data_frame = TRUE,
                       force_download = FALSE) {

  # Step 1= Determine the full path for the cached file.
  # This path is OS-agnostic and user-specific.
  cached_file_path <- .get_cache_path(table_name = table_name, year = year)

  # Step 2= Check if the file needs to be downloaded.
  # This occurs if `force_download` is TRUE or if the cached file doesn't exist.
  if (force_download || !file.exists(cached_file_path)) {
    message(
      ifelse(force_download,
             "Forcing re-download from S3...",
             "No local cache found or download is forced. Downloading from S3...")
    )

    # Construct the S3 URL.
    # Note= This assumes a specific URL structure on the S3 server.
    # Example= .../4_cleaned/1400_food.parquet
    file_name_on_s3 <- paste0(year, "_", table_name, ".parquet")
    base_s3_url <- "https://s3.ir-tbz-sh1.arvanstorage.ir/iran-open-data/HBSIR/4_cleaned"
    s3_url_to_download <- file.path(base_s3_url, file_name_on_s3)

    # Step 3= Call the helper function to download the file to the cache path.
    .download_to_cache(s3_url = s3_url_to_download, destination_path = cached_file_path)
    message("Download complete. Data saved to cache= ", cached_file_path)

  } else {
    # If the file exists in the cache and download is not forced, load from cache.
    message("Loading data from local cache= ", cached_file_path)
  }

  # Step 4= Read the Parquet file (either newly downloaded or from cache)
  # into an Arrow Table object.
  data_table <- tryCatch({
    arrow::read_parquet(cached_file_path)
  }, error = function(e) {
    stop(paste0(
      "Failed to read the Parquet file from '", cached_file_path, "'.\n",
      "The file might be corrupted or not a valid Parquet file.\n",
      "Original error= ", e$message
    ))
  })

  # Step 5= Convert to data.frame if requested, otherwise return the Arrow Table.
  if (as_data_frame) {
    message("Converting Arrow Table to R data.frame...")
    df <- as.data.frame(data_table)
    df$year <- as.numeric(year)      # <-- Add this line
    df$name<- as.character(table_name)
    return(df)
  } else {
    message("Returning data as an Arrow Table.")
    return(data_table)
  }
}
