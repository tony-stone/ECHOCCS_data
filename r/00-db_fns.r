getDBConn <- function(connection_details) {
  return(do.call(DBI::dbConnect, connection_details))
}

getRowCounts <- function(conn, tbl_names) {

  sql_queries <- paste0("SELECT COUNT(*) FROM dbo.", tbl_names, ";")

  counts_list <- lapply(sql_queries, function(query, conn) {
    executeQuery(conn,
                 sql_query = query,
                 print_progress = FALSE)

    return(executeQuery(conn, sql_query = query, print_progress = FALSE))
  }, conn = conn)

  counts <- unlist(counts_list)
  names(counts) <- tbl_names

  return(counts)
}

getEchildTableNames <- function(conn,
                                dataset_name,
                                get_row_counts = FALSE,
                                apply_exclusions = TRUE) {

  supported_datasets <- data.table::data.table(theme = toupper(c("birth_notifications",
                                                                 "birth_reg",
                                                                 "death_reg",
                                                                 paste0("HES_",
                                                                        c("APC", "OP", "AE")))),
                                               keywords = c("_birth_notifications",
                                                            "_civil_reg_births",
                                                            "_civil_reg_deaths",
                                                            paste0("HES_",
                                                                   c("APC", "OP", "AE"))))

  exclusions <- data.table::data.table(theme = toupper(c("deaths")),
                                       exclude = c("FILE0184888_civil_reg_deaths"))

  dataset_name <- toupper(dataset_name)

  if(length(dataset_name) != 1) stop("'dataset_name' name must be a character vector of length 1.")
  if(!(dataset_name %in% supported_datasets$theme)) stop(paste("Dataset name not supported. Must be one of:\n",
                                                               paste0(unique(supported_datasets$theme),
                                                                      collapse = "\n")))
  if(length(get_row_counts) != 1 |
     !(get_row_counts %in% c(FALSE, TRUE))) stop("'get_row_counts' must be TRUE or FALSE only.")

  db_table_names <- DBI::dbListTables(conn, schema = "dbo")

  search_text <- paste0(supported_datasets[theme == dataset_name, keywords],
                        collapse = "|")
  table_names <- db_table_names[grep(search_text,
                                     db_table_names,
                                     ignore.case = TRUE)]

  if(apply_exclusions) {
    table_names <- table_names[!(table_names %in% exclusions[theme == dataset_name, exclude])]
  }

  if(length(table_names) == 0) return(FALSE)

  if(get_row_counts) {
    return(getRowCounts(conn, table_names))
  } else {
    return(table_names)
  }
}

executeQuery <- function(conn,
                         sql_query,
                         query_type = "GET",
                         print_progress = TRUE) {

  if(typeof(query_type) != "character" |
     length(query_type) != 1) stop("'query_type' must be a character vector of length 1.")

  query_type <- toupper(query_type)
  if(!(toupper(query_type) %in% c("GET", "SEND"))) stop("'query_type' must be either 'GET' or 'SEND'.")

  if(nchar(sql_query) > 65) {
    sql_query_msg <- paste0(substring(sql_query, 1, 61),
                            " ...")
  } else {
    sql_query_msg <- substring(sql_query, 1, 61)
  }

  if(print_progress) cat("Executing SQL", query_type, "query:", sql_query_msg)

  tictoc::tic(paste("SQL", query_type, "query: "))

  if(query_type == "GET") {
    result <- DBI::dbGetQuery(conn, sql_query)
  } else if (query_type == "SEND") {
    result <- DBI::dbSendQuery(conn, sql_query)
  }

  tictoc::toc(quiet = !print_progress,
              func.toc = tictocSqlQueryMsg,
              info = "INFO")

  return(result)
}


tictocSqlQueryMsg <- function(tic, toc, msg, info) {
  if(is.null(msg) || is.na(msg) || length(msg) == 0) {
    outmsg <- paste0("\n\n", tictocTimeFormat(toc - tic), " elapsed.")
  } else {
    outmsg <- paste0("\n\n", info, ": ", msg,
                     tictocTimeFormat(toc - tic),
                     " elapsed.")
  }

  return(outmsg)
}

tictocTimeFormat <- function(time_elapsed) {

  plural <- c("", "s")
  time_elapsed_msg <- ""

  if(time_elapsed > 60) {
    if(time_elapsed > 60^2) {
      hours <- floor(time_elapsed / 60^2)
      time_elapsed <- time_elapsed - hours * 60^2
      time_elapsed_msg <- paste0(hours, " hour",
                                 plural[as.integer(hours > 1) + 1L],
                                 ", ")
    }
    mins <- floor(time_elapsed / 60)
    time_elapsed <- time_elapsed - mins * 60
    time_elapsed_msg <- paste0(time_elapsed_msg,
                               mins, " minute",
                               plural[as.integer(mins > 1) + 1L],
                               ", ")
  }
  time_elapsed_msg <- paste0(time_elapsed_msg,
                             round(time_elapsed, 3), " second",
                             plural[as.integer(time_elapsed != 1) + 1L])

  return(time_elapsed_msg)
}
