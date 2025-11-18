#' Add classification labels to a data frame for commodities, occupations, or industries
#'
#' This function reads metadata YAML files from the Iran Open Data GitHub repository,
#' flattens hierarchical codes (including ranges and year-specific codes) into a lookup table,
#' and joins descriptive classification attributes to the input data frame. For commodities,
#' classifications are split by year: before 1383 and after 1383, with post-1383 classifications
#' based on year rather than commodity names.
#'
#' @param df A data frame containing a column of classification codes.
#' @param code_col A string: name of the column in `df` containing the classification codes.
#' @param classification_type A string: type of classification ("commodity", "occupation", or "industry").
#' @param year Optional integer: survey year to match year-specific codes (default: NULL, uses 1383 for years >= 1383).
#' @param level Optional integer or integer vector: only include classifications at this hierarchical level(s).
#' @return A data frame with the original columns plus `level`, `label`, `farsi_name`, and `name` columns.
#' @import dplyr yaml purrr stringr readr
#' @export
#' @examples
#' \dontrun{
#'   df <- load.expenditure(1389, summarise = FALSE)
#'   df <- hbsir.add_classification(df, code_col = "Commodity_Code", classification_type = "commodity", year = 1389)
#'   df <- hbsir.add_classification(df, code_col = "Occupation_Code", classification_type = "occupation", year = 1389)
#'   df <- hbsir.add_classification(df, code_col = "Industry_Code", classification_type = "industry", year = 1389)
#'   head(df)
#' }
hbsir.add_classification <- function(df, code_col, classification_type = c("commodity", "occupation", "industry"), year = NULL, level = NULL) {
  # Validate inputs
  classification_type <- match.arg(classification_type)
  if (!code_col %in% names(df)) {
    stop("The specified code column '", code_col, "' does not exist in the data frame.")
  }

  # URLs for metadata YAML files
  metadata_urls <- list(
    commodity = "https://raw.githubusercontent.com/Iran-Open-Data/HBSIR/master/hbsir/metadata/commodities.yaml",
    occupation = "https://raw.githubusercontent.com/Iran-Open-Data/BSSIR/master/bssir/metadata/occupations.yaml",
    industry = "https://raw.githubusercontent.com/Iran-Open-Data/BSSIR/master/bssir/metadata/industries.yaml"
  )

  metadata_url <- metadata_urls[[classification_type]]

  # Read the YAML file with error handling
  tryCatch({
    raw_meta <- yaml::read_yaml(metadata_url)
    message("YAML file for ", classification_type, " loaded successfully. Number of entries: ", length(raw_meta))
  }, error = function(e) {
    stop("Failed to read ", classification_type, " metadata from '", metadata_url, "': ", e$message)
  })

  if (classification_type == "commodity") {
    effective_section <- if (is.null(year) || year < 1383) "sci_coicop_1363" else "sci_coicop_1383"
    if (!effective_section %in% names(raw_meta)) {
      stop("YAML does not contain the expected section: ", effective_section)
    }
    raw_meta <- raw_meta[[effective_section]]$items
  }

  # Recursive function to extract codes from nested structures with proper formatting
  extract_codes <- function(code_data, parent_level = NULL, parent_name = NULL) {
    codes <- character(0)
    if (is.list(code_data)) {
      if (all(c("start", "end") %in% names(code_data))) {
        if (classification_type == "commodity") {
          start_str <- code_data$start
          end_str <- code_data$end
          start_no_ <- gsub("_", "", start_str)
          end_no_ <- gsub("_", "", end_str)
          start_num <- as.integer(start_no_)
          end_num <- as.integer(end_no_)
          if (!is.na(start_num) && !is.na(end_num) && start_num < end_num) {
            seq_nums <- seq(start_num, end_num - 1)
            seq_codes <- sapply(seq_nums, function(n) {
              n_str <- sprintf("%05d", n)
              sprintf("%s_%s", substr(n_str, 1, 2), substr(n_str, 3, 5))
            })
            codes <- c(codes, seq_codes)
          }
        } else {
          # For occupation and industry: assume numeric codes, sequence from start to end-1, pad with zeros
          start_i <- as.integer(code_data$start)
          end_i <- as.integer(code_data$end)
          if (end_i <= start_i) return(character(0))
          seq_nums <- seq(start_i, end_i - 1)
          width <- max(nchar(as.character(code_data$start)), nchar(as.character(code_data$end)))
          seq_codes <- sprintf(paste0("%0", width, "d"), seq_nums)
          codes <- c(codes, seq_codes)
        }
      } else if (length(code_data) > 0 && !is.null(names(code_data))) {
        # Handle year-specific codes or nested items
        for (key in names(code_data)) {
          if (grepl("^[0-9]{4}$", key)) {
            # Year-specific codes
            year_int <- as.integer(key)
            if (is.null(year)) {
              year <- if (is.null(parent_level) || parent_level >= 1) 1383 else 1363
            }
            if (year_int <= year) {
              year_codes <- code_data[[key]]
              if (is.character(year_codes)) {
                codes <- c(codes, year_codes)
              } else if (is.list(year_codes)) {
                nested_codes <- extract_codes(year_codes, parent_level, key)
                codes <- c(codes, nested_codes)
              }
            }
          } else {
            # Nested items or other lists
            nested_codes <- extract_codes(code_data[[key]], parent_level, key)
            codes <- c(codes, nested_codes)
          }
        }
      }
    } else {
      # Single code: convert to character, pad if not commodity
      if (classification_type == "commodity") {
        codes <- as.character(code_data)
      } else {
        codes <- sprintf("%04d", code_data)
      }
    }
    return(unique(codes))
  }

  # Flatten the hierarchical metadata into a lookup table
  flatten_classification_metadata <- function(meta) {
    lookup <- purrr::map_dfr(seq_along(meta), function(i) {
      entry <- meta[[i]]
      message("Processing ", classification_type, " entry ", i, ": full entry = ", paste(capture.output(str(entry)), collapse = "\n"))

      # Handle versions if present
      if (!is.null(entry$versions)) {
        version_keys <- names(entry$versions)[grepl("^[0-9]{4}$", names(entry$versions))]
        if (length(version_keys) > 0) {
          version_ints <- as.integer(version_keys)
          if (is.null(year)) {
            year_temp <- if (classification_type == "commodity" && grepl("1383", effective_section)) 1383 else 1363
          } else {
            year_temp <- year
          }
          selected_version_int <- max(version_ints[version_ints <= year_temp], na.rm = TRUE)
          if (!is.infinite(selected_version_int)) {
            selected_version <- as.character(selected_version_int)
            selected <- entry$versions[[selected_version]]
            if (!is.null(selected$label)) entry$label <- selected$label
            if (!is.null(selected$name)) entry$name <- selected$name
            if (!is.null(selected$farsi_name)) entry$farsi_name <- selected$farsi_name
            # Add handling for other fields if needed
          }
        }
      }

      codes <- character(0)
      if (!is.null(entry$code)) {
        codes <- extract_codes(entry$code, entry$level, names(entry))
      } else if (!is.null(entry$items)) {
        # Extract codes from nested items
        items_codes <- purrr::map_dfr(entry$items, function(item) {
          # Handle versions for item
          if (!is.null(item$versions)) {
            version_keys <- names(item$versions)[grepl("^[0-9]{4}$", names(item$versions))]
            if (length(version_keys) > 0) {
              version_ints <- as.integer(version_keys)
              if (is.null(year)) {
                year_temp <- if (classification_type == "commodity" && grepl("1383", effective_section)) 1383 else 1363
              } else {
                year_temp <- year
              }
              selected_version_int <- max(version_ints[version_ints <= year_temp], na.rm = TRUE)
              if (!is.infinite(selected_version_int)) {
                selected_version <- as.character(selected_version_int)
                selected <- item$versions[[selected_version]]
                if (!is.null(selected$label)) item$label <- selected$label
                if (!is.null(selected$name)) item$name <- selected$name
                if (!is.null(selected$farsi_name)) item$farsi_name <- selected$farsi_name
                # Add handling for other fields if needed
              }
            }
          }

          item_codes <- if (!is.null(item$code)) extract_codes(item$code, entry$level, names(entry$items)) else character(0)
          if (length(item_codes) > 0 || !is.null(item$level)) {
            label <- item$label %||% NA_character_
            # Handle farsi_name as character, flattening lists if necessary
            farsi_name_value <- if (is.list(item$farsi_name)) {
              if (length(item$farsi_name) > 0) paste(item$farsi_name, collapse = ", ") else NA_character_
            } else if (is.character(item$farsi_name)) {
              item$farsi_name
            } else {
              NA_character_
            }
            tibble::tibble(
              code = item_codes,
              level = if (!is.null(item$level)) item$level else if (!is.null(entry$level)) entry$level else NA_integer_,
              label = label,
              farsi_name = farsi_name_value,
              name = if (!is.null(item$name)) item$name else if (!is.null(item$title)) item$title else NA_character_
            )
          } else {
            tibble::tibble()
          }
        })
        return(items_codes)
      }

      if (length(codes) > 0) {
        label <- entry$label %||% NA_character_
        # Handle farsi_name as character
        farsi_name_value <- if (is.list(entry$farsi_name)) {
          if (length(entry$farsi_name) > 0) paste(entry$farsi_name, collapse = ", ") else NA_character_
        } else if (is.character(entry$farsi_name)) {
          entry$farsi_name
        } else if (is.character(entry$farsi_title)) {
          entry$farsi_title
        } else {
          NA_character_
        }
        tibble::tibble(
          code = codes,
          level = if (!is.null(entry$level)) entry$level else NA_integer_,
          label = label,
          farsi_name = farsi_name_value,
          name = if (!is.null(entry$name)) entry$name else if (!is.null(entry$title)) entry$title else NA_character_
        )
      } else {
        message("No valid codes generated for ", classification_type, " entry ", i)
        tibble::tibble()
      }
    })
    message("Lookup table for ", classification_type, " created with ", nrow(lookup), " rows.")
    return(lookup)
  }

  # Create the lookup table
  lookup <- flatten_classification_metadata(raw_meta)

  # Filter by level if requested
  if (!is.null(level)) {
    lookup <- lookup %>% dplyr::filter(level %in% level)
  }

  # Debug: Print sample of lookup codes
  if (nrow(lookup) > 0) {
    message("Sample codes in lookup['code']: ", paste(head(lookup$code), collapse = ", "))
  } else {
    message("No codes found in lookup table for ", classification_type, ". The YAML may not contain codes for year ", year, " or structure needs adjustment.")
  }

  # Standardize df code column format to match YAML
  if (classification_type == "commodity") {
    df <- df %>% dplyr::mutate(
      !!code_col := case_when(
        stringr::str_length(!!rlang::sym(code_col)) == 4 ~ stringr::str_c(stringr::str_sub(!!rlang::sym(code_col), 1, 1), "_", stringr::str_sub(!!rlang::sym(code_col), 2, 4)),
        stringr::str_length(!!rlang::sym(code_col)) == 5 ~ stringr::str_c(stringr::str_sub(!!rlang::sym(code_col), 1, 2), "_", stringr::str_sub(!!rlang::sym(code_col), 3, 5)),
        stringr::str_length(!!rlang::sym(code_col)) == 6 ~ stringr::str_c(stringr::str_sub(!!rlang::sym(code_col), 1, 2), "_", stringr::str_sub(!!rlang::sym(code_col), 3, 6)),
        TRUE ~ as.character(!!rlang::sym(code_col))
      )
    )
  } else {
    # For occupation and industry, pad to 4 digits if necessary
    df <- df %>% dplyr::mutate(
      !!code_col := sprintf("%04s", as.character(!!rlang::sym(code_col)))
    )
  }
  message("After standardization, unique codes in df['", code_col, "']: ", paste(head(unique(df[[code_col]]), 10), collapse = ", "))

  # Ensure the code column in df is character
  df <- df %>% dplyr::mutate(!!code_col := as.character(!!rlang::sym(code_col)))

  # Perform left join with many-to-many relationship
  if (nrow(lookup) > 0) {
    df <- df %>%
      dplyr::left_join(lookup, by = setNames("code", code_col), relationship = "many-to-many")
  } else {
    message("Warning: No lookup data available for ", classification_type, ". Returning df without classification columns.")
  }

  # Debug: Check if any matches occurred
  matched <- if (nrow(lookup) > 0) sum(!is.na(df$level)) else 0
  message("Number of rows with matched ", classification_type, " classifications: ", matched, " out of ", nrow(df))

  return(df)
}
