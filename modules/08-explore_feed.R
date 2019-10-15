# Output:
# - explore_feed_daily

if (!file.exists(file.path(data_dir, "explore_feed_monthly.csv"))) {
  message("Fetching Explore Feed stats")
  feed_monthly_query <- "SET mapreduce.map.memory.mb=4096;
    SELECT
      COUNT(DISTINCT event.app_install_id) AS n_users
    FROM event.MobileWikiAppFeed
    WHERE year = ${year} AND month = ${month}
      AND revision = 18115458
      AND useragent.os_family = 'Android'
      AND INSTR(useragent.wmf_app_version, '-r-') > 0
      AND event.action = 'exit'"
  explore_feed_monthly <- wmf::query_hive(glue(feed_monthly_query, .open = "${"))
  readr::write_csv(explore_feed_monthly, file.path(data_dir, "explore_feed_monthly.csv"))
} else {
  explore_feed_monthly <- readr::read_csv(file.path(data_dir, "explore_feed_monthly.csv"), col_types = "i")
}

if (!file.exists(file.path(data_dir, "explore_feed_daily.csv"))) {
  feed_daily_query <- "WITH per_user_avgs AS (
      SELECT
        event.app_install_id,
        AVG(event.time_spent) AS avg_time_spent
      FROM event.MobileWikiAppFeed
      WHERE year = ${year} AND month = ${month} AND day = ${day}
        AND revision = 18115458
        AND useragent.os_family = 'Android'
        AND INSTR(useragent.wmf_app_version, '-r-') > 0
        AND event.action = 'exit'
      GROUP BY event.app_install_id
    )
    SELECT
      COUNT(1) AS n_users,
      ROUND(AVG(avg_time_spent), 1) AS avg_time_spent
    FROM per_user_avgs"
  explore_feed_daily <- purrr::map_dfr(
    seq(start_date, end_date, by = "day"),
    function(date) {
      message("Fetching data from ", date)
      c(year, month, day) %<-% wmf::extract_ymd(date)
      query <- glue(feed_daily_query, .open = "${")
      result <- wmf::query_hive(query)
      result$date <- date
      return(result)
    }
  )
  readr::write_csv(explore_feed_daily, file.path(data_dir, "explore_feed_daily.csv"))
} else {
  explore_feed_daily <- readr::read_csv(file.path(data_dir, "explore_feed_daily.csv"), col_types = "idD")
}
