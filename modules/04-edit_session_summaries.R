# Output:
# - edit_session_summaries
# - clientside_contributions
# - contributors_session_length
# - contributors_daily
# - contributors_monthly

if (!file.exists(file.path(data_dir, "edit_session_summaries.csv"))) {
  edits_query <- "SELECT
      event.app_install_id,
      event.session_token AS edit_session_token,
      CASE WHEN event.wikidataDescriptionEdit IS NOT NULL AND event.pageNS = 'FILE' THEN 'image caption'
           WHEN event.wikidataDescriptionEdit IS NOT NULL THEN 'title description'
           WHEN event.pageNS IS NULL THEN 'article'
           ELSE 'other' END AS edit_type,
      -- MIN(event.client_dt) AS edit_session_start_dt,
      IF(MAX(event.anon), 'TRUE', 'FALSE') AS is_anon,
      IF(SUM(IF(event.action = 'start', 1, 0)) = 1, 'TRUE', 'FALSE') AS edit_session_started,
      IF(SUM(IF(event.action = 'saveAttempt', 1, 0)) = 1, 'TRUE', 'FALSE') AS edit_save_attempted,
      IF(SUM(IF(event.action = 'saved', 1, 0)) = 1, 'TRUE', 'FALSE') AS edit_session_completed
    FROM event.MobileWikiAppEdit
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      AND revision = 18115551
      AND useragent.os_family = 'Android'
      AND INSTR(useragent.wmf_app_version, '-r-') > 0
      AND event.action IN('start', 'saveAttempt', 'saved')
    GROUP BY
      event.app_install_id, event.session_token,
      CASE WHEN event.wikidataDescriptionEdit IS NOT NULL AND event.pageNS = 'FILE' THEN 'image caption'
           WHEN event.wikidataDescriptionEdit IS NOT NULL THEN 'title description'
           WHEN event.pageNS IS NULL THEN 'article'
           ELSE 'other' END"
  message("Fetching editing stats")
  edit_session_summaries <- purrr::map_dfr(
    seq(start_date, end_date, by = "day"),
    function(date) {
      message("Fetching data from ", date)
      c(year, month, day) %<-% wmf::extract_ymd(date)
      query <- glue(edits_query, .open = "${")
      result <- wmf::query_hive(query)
      result$date <- date
      result <- result[result$edit_session_started, ]
      result$edit_session_started <- NULL
      return(result)
    }
  )
  readr::write_csv(edit_session_summaries, file.path(data_dir, "edit_session_summaries.csv"))
} else {
  edit_session_summaries <- readr::read_csv(file.path(data_dir, "edit_session_summaries.csv"), col_types = "ccclllD")
}

clientside_contributions <- edit_session_summaries %>%
  keep_where(edit_session_completed) %>%
  count(app_install_id, edit_type) %>%
  spread(edit_type, n, fill = 0) %>%
  mutate(total = article + `title description`)

contributors_session_length <- clientside_contributions %>%
  dplyr::left_join(session_summaries, by = "app_install_id") %>%
  keep_where(!is.na(n_total_sessions)) %>%
  mutate(avg_session_time = total_session_length / n_total_sessions) %>%
  summarize(avg_session_time = mean(avg_session_time)) %>%
  pull(avg_session_time) %>%
  round %>%
  lubridate::seconds_to_period() %>%
  tolower

contributors_daily <- edit_session_summaries %>%
  keep_where(edit_session_completed) %>%
  group_by(date) %>%
  summarize(n_users = length(unique(app_install_id))) %>%
  summarize(avg_users = mean(n_users)) %>%
  pull(avg_users) %>%
  ceiling

contributors_monthly <- edit_session_summaries %>%
  keep_where(edit_session_completed) %>%
  summarize(n_users = length(unique(app_install_id))) %>%
  pull(n_users)
