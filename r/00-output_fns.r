saveSrsOutput <- function(data,
                          fname,
                          count_field = "cnt",
                          date = Sys.Date()) {

  dt <- copy(data)

  setnames(dt, count_field, "..cnt..")

  dt[..cnt.. < 10, ..cnt_temp.. := "*"]
  dt[..cnt.. >= 10, ..cnt_temp.. := as.character(..cnt..)]
  dt[, ':=' (..cnt.. = ..cnt_temp..,
             ..cnt_temp.. = NULL)]

  setnames(dt, "..cnt..", count_field)

  fwrite(dt,
         file = paste0("data-out/",
                       format(date, "%Y-%m-%d_"),
                       fname,
                       ".csv"))

}
