writeParquetMultiFile <- function(data,
                                  path,
                                  partitioning) {

  write_result <- tryCatch({
    arrow::write_dataset(data,
                         path = path,
                         format = "parquet",
                         partitioning = partitioning)
    path
  },
  error = function(e) {
    cat("Error:",
        conditionMessage(e),
        "\n")
    as.character(NA)
  })

  return(write_result)
}

writeParquetFile <- function(data,
                             dir_path,
                             filename) {

  if(!dir.exists(dir_path)) dir.create(dir_path,
                                       recursive = TRUE)


  file_path <- paste0(dir_path,
                      "/",
                      filename,
                      ".parquet")

  if(data.table::is.data.table(data)) {
    warning("Data to be written is a data.table. This may be inefficient when saved to Parquet format.")
  }

  write_result <- tryCatch({
    arrow::write_parquet(data,
                         sink = file_path)
    file_path
  },
  error = function(e) {
    cat("Error:",
        conditionMessage(e),
        "\n")
    as.character(NA)
  })

  return(write_result)
}


duckDbWriteTable <- function(duck_conn,
                             table_name,
                             dir_path,
                             data_filename) {

  if(!dir.exists(dir_path)) dir.create(dir_path,
                                       recursive = TRUE)


  file_path <- paste0(dir_path, "/",
                      data_filename,
                      ".parquet")

  write_result <- tryCatch({
    DBI::dbSendQuery(duck_conn,
                     paste0("COPY ",
                            table_name,
                            " TO '",
                            file_path,
                            "' (FORMAT parquet);"))
    file_path
  },
  error = function(e) {
    cat("Error:",
        conditionMessage(e),
        "\n")
    as.character(NA)
  })

  return(write_result)
}


deleteFolderOfFile <- function(path,
                               filename_without_dot = FALSE) {

  if(filename_without_dot | grepl(".", basename(path), fixed = TRUE)) {
    unlink(dirname(path),
           recursive = TRUE)
  } else {
    unlink(path,
           recursive = TRUE)
  }
}


writeTempParquetFile <- function(data) {
  temp_file_path <- tempfile(fileext = ".parquet")

  temp_file_path <- writeParquetFile(data,
                                     dirname(temp_file_path),
                                     tools::file_path_sans_ext(basename(temp_file_path)))

  return(temp_file_path)
}
