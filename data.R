source("modules/1_setup.R")

system("hive -f android_app_sessions_subset.hql")

query <- "USE bearloga;
INSERT OVERWRITE TABLE `android_app_sessions_subset`
PARTITION (day = ${day})
SELECT
    app_install_id,
    MAX(languages) AS languages,
    SUM(IF(session_length >= 0, 1, 0)) AS n_total_sessions,
    SUM(IF(session_length >= 0 AND total_articles > 0, 1, 0)) AS n_reading_sessions,
    SUM(IF(session_length >= 0, session_length, 0)) AS total_session_length,
    SUM(IF(session_length >= 0 AND total_articles > 0, session_length, 0)) AS total_reading_length,
    SUM(IF(session_length >= 0, total_articles, 0)) AS total_pages_read
FROM (
    SELECT DISTINCT
        event.app_install_id,
        event.session_token,
        event.languages,
        event.length AS session_length,
        event.totalpages AS total_articles
    FROM event.MobileWikiAppSessions
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      -- AND revision = 18948969
      AND INSTR(useragent.wmf_app_version, '-r-') > 0
      AND event.app_install_id IS NOT NULL
      AND event.session_token IS NOT NULL
) AS unique_sessions
GROUP BY app_install_id
HAVING n_total_sessions > 0;

SHOW PARTITIONS android_app_sessions_subset;"

results <- purrr::walk(
  seq(start_date, end_date, by = "day"),
  function(date) {
    message("Fetching data from ", date)
    c(year, month, day) %<-% wmf::extract_ymd(date)
    query <- glue(query, .open = "${", .close = "}")
    result <- tail(wmf::query_hive(query))[, 1]
  }
)
