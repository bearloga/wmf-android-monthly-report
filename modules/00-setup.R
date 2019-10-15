library(glue)
library(zeallot)
library(magrittr)
import::from(dplyr, group_by, summarize, ungroup, mutate, select, arrange, keep_where = filter, rename, count, pull)
import::from(tidyr, spread, gather)
import::from(polloi, compress)
options(digits = 2, datatable.fread.input.cmd.message = FALSE)

# Intelligently figure out the year & month to calculate stats for:
today <- lubridate::today()
beginning_of_curr_month <- lubridate::floor_date(today, "month")
last_day_of_prev_month <- beginning_of_curr_month - 1
year <- lubridate::year(last_day_of_prev_month)
month <- lubridate::month(last_day_of_prev_month)

# Set up organization for data:
yyyy_mm <- sprintf("%.0f-%02.0f", year, month)
data_dir <- file.path("data", yyyy_mm)

# Calculate date range based on year & month given above:
start_date <- as.Date(paste(year, month, "1", sep = "-"), format = "%Y-%m-%d")
end_date <- start_date + months(1) - 1

fs::dir_create(data_dir)

message(glue("Report to be generated for {month.name[month]} {year}, fetching data from {start_date} to {end_date}"))
