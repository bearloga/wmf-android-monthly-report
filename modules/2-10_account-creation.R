# Output:
# - accounts_created

if (!file.exists(file.path(data_dir, "accounts_created.csv"))) {
  ssac <- "SELECT day, COUNT(1) AS n_new_accounts
FROM event.ServerSideAccountCreation
WHERE year = ${year} AND month = ${month}
  AND useragent.os_family = 'Android'
  AND INSTR(useragent.wmf_app_version, '-r-') > 1
  AND event.isApi
  AND event.isSelfMade
GROUP BY day;"
  accounts_created <- wmf::query_hive(glue(ssac, .open = "${"))
  readr::write_csv(accounts_created, file.path(data_dir, "accounts_created.csv"))
} else {
  accounts_created <- readr::read_csv(file.path(data_dir, "accounts_created.csv"))
}
