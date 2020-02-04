if (!file.exists(file.path(data_dir, "daily_stats.csv.gz"))) {
  stats_query <- "SET mapreduce.map.memory.mb=4096;
    SELECT
      app_install_id,
      is_anon,
      languages,
      days_installed
    FROM (
      SELECT
        event.app_install_id,
        IF(event.is_anon, 'TRUE', 'FALSE') AS is_anon,
        event.languages,
        event.appInstallAgeDays AS days_installed,
        RANK() OVER (PARTITION BY event.app_install_id ORDER BY event.appInstallAgeDays DESC) AS stat_rank
      FROM event.MobileWikiAppDailyStats
      WHERE year = ${year} AND month = ${month}
        AND revision = 18115101
        AND useragent.os_family = 'Android'
        AND INSTR(useragent.wmf_app_version, '-r-') > 0
    ) AS ranked_stats
    WHERE stat_rank = 1 -- latest stats from each device"
  message("Fetching daily stats")
  daily_stats <- wmf::query_hive(glue(stats_query, .open = "${")) %>%
    mutate(n_languages = nchar(languages) - nchar(gsub(",", "", languages)) + 1) %>%
    arrange(app_install_id, dplyr::desc(days_installed)) %>%
    dplyr::distinct(app_install_id, .keep_all = TRUE) %>%
    mutate(multilang = n_languages > 1)
  readr::write_csv(daily_stats, file.path(data_dir, "daily_stats.csv"))
  system(glue("gzip {data_dir}/daily_stats.csv"))
} else {
  daily_stats <- readr::read_csv(file.path(data_dir, "daily_stats.csv.gz"), col_types = "clciil")
}

# Compute stats for the metrics table at the end:
multilang_stat <- daily_stats %>%
  count(multilang) %>%
  spread(multilang, n) %>%
  mutate(prop = `TRUE` / (`TRUE` + `FALSE`)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(`TRUE`, 1), compress(`TRUE` + `FALSE`, 1))) %>%
  pull(summarized)

anon_stat <- daily_stats %>%
  count(is_anon) %>%
  spread(is_anon, n) %>%
  mutate(prop = `TRUE` / (`TRUE` + `FALSE`)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(`TRUE`, 1), compress(`TRUE` + `FALSE`, 1))) %>%
  pull(summarized)

loggedin_stat <- daily_stats %>%
  count(is_anon) %>%
  spread(is_anon, n) %>%
  mutate(prop = `FALSE` / (`TRUE` + `FALSE`)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(`FALSE`, 1), compress(`TRUE` + `FALSE`, 1))) %>%
  pull(summarized)
