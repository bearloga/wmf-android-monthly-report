# Output:
# - reading_time
# - readers_monthly
# - readers_daily

if (!file.exists(file.path(data_dir, "reading_time.csv"))) {
  reading_query <- "SET mapreduce.map.memory.mb=4096;
    SELECT
      event.app_install_id,
      ROUND(AVG(event.time_spent), 1) AS avg_reading_time
    FROM event.MobileWikiAppLinkPreview
    WHERE year = ${year} AND month = ${month}
      AND revision = 18531254
      AND event.action = 'navigate'
      AND event.page_id > 0
      AND useragent.os_family = 'Android'
      AND INSTR(useragent.wmf_app_version, '-r-') > 0
    GROUP BY event.app_install_id"
  message("Fetching reading stats")
  reading_time <- wmf::query_hive(glue(reading_query, .open = "${"))
  readr::write_csv(reading_time, file.path(data_dir, "reading_time.csv"))
} else {
  reading_time <- readr::read_csv(file.path(data_dir, "reading_time.csv"), col_types = "cd")
}

monthly_query <- "USE bearloga;
WITH reading_users AS (
  SELECT
    app_install_id,
    SUM(IF(n_reading_sessions > 0, 1, 0)) > 0 AS is_reader
  FROM android_app_sessions_subset
  WHERE day >= 1
  GROUP BY app_install_id
)
SELECT is_reader, COUNT(1) AS n_users
FROM reading_users
GROUP BY is_reader;"
readers_monthly <- wmf::query_hive(monthly_query) %>%
  spread(is_reader, n_users) %>%
  mutate(prop = true / (true + false)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(true, 1), compress(true + false, 1))) %>%
  pull(summarized)

daily_query <- "USE bearloga;
WITH reading_users AS (
  SELECT
    day, app_install_id,
    SUM(IF(n_reading_sessions > 0, 1, 0)) > 0 AS is_reader
  FROM android_app_sessions_subset
  WHERE day >= 1
  GROUP BY day, app_install_id
)
SELECT day, is_reader, COUNT(1) AS n_users
FROM reading_users
GROUP BY day, is_reader;"
readers_daily <- wmf::query_hive(daily_query) %>%
  spread(is_reader, n_users) %>%
  summarize(true = mean(true), false = mean(false)) %>%
  mutate(prop = true / (true + false)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(true, 1), compress(true + false, 1))) %>%
  pull(summarized)
