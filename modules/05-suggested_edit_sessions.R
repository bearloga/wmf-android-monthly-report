# Output:
# - se_time_spent
# - se_session_length

if (!file.exists(file.path(data_dir, "suggested_edit_sessions.csv"))) {
  se_session_query <- "USE event;
    SELECT
      event.app_install_id AS install_id,
      event.session_token AS session_id,
      -- Session summary:
      SUM(event.time_spent) AS total_time_spent, -- in seconds
      COALESCE(SUM(description_addition.successes) + SUM(description_translation.successes) + SUM(caption_addition.successes) + SUM(caption_translation.successes), 0L) AS n_edits
    FROM MobileWikiAppSuggestedEdits
    LATERAL VIEW
      JSON_TUPLE(event.edit_tasks, 'add-description', 'translate-description', 'add-caption', 'translate-caption') edit_tasks
      AS add_desc, translate_desc, add_cap, translate_cap
    LATERAL VIEW JSON_TUPLE(edit_tasks.add_desc, 'successes') description_addition AS successes
    LATERAL VIEW JSON_TUPLE(edit_tasks.translate_desc, 'successes') description_translation AS successes
    LATERAL VIEW JSON_TUPLE(edit_tasks.add_cap, 'successes') caption_addition AS successes
    LATERAL VIEW JSON_TUPLE(edit_tasks.translate_cap, 'successes') caption_translation AS successes
    WHERE year = ${year} AND month = ${month}
      AND INSTR(useragent.wmf_app_version, '-r-') > 0
    GROUP BY event.app_install_id, event.session_token;"
  message("Fetching Suggested Edits stats")
  suggested_edit_sessions <- wmf::query_hive(glue(se_session_query, .open = "${"))
  readr::write_csv(suggested_edit_sessions, file.path(data_dir, "suggested_edit_sessions.csv"))
} else {
  suggested_edit_sessions <- readr::read_csv(file.path(data_dir, "suggested_edit_sessions.csv"), col_types = "ccii")
}

se_time_spent <- suggested_edit_sessions %>%
  group_by(install_id) %>%
  summarize(avg_session_time = sum(total_time_spent) / dplyr::n()) %>%
  summarize(avg_session_time = median(avg_session_time)) %>%
  pull(avg_session_time) %>%
  round %>%
  lubridate::seconds_to_period() %>%
  tolower

# Suggested Edits users' session lengths:
if (!file.exists(file.path(data_dir, "suggested_edit_sessions2.csv.gz"))) {
  se_session_query2 <- "USE event;
    WITH suggested_edit_sessions AS (
      SELECT DISTINCT
        event.app_install_id AS install_id,
        event.session_token AS session_id
      FROM MobileWikiAppSuggestedEdits
      WHERE year = ${year} AND month = ${month} AND day = ${day}
        AND INSTR(useragent.wmf_app_version, '-r-') > 0
    )
    SELECT
      install_id, session_id, sessions.event.length AS session_length
    FROM suggested_edit_sessions AS se_sessions
    LEFT JOIN MobileWikiAppSessions AS sessions ON (
      se_sessions.install_id = sessions.event.app_install_id
      AND se_sessions.session_id = sessions.event.session_token
      AND sessions.year = ${year} AND sessions.month = ${month} AND sessions.day = ${day}
      AND sessions.revision = 18948969
    )
    WHERE sessions.event.length >= 0"
  message("Fetching session lengths for Suggested Edits users")
  suggested_edit_sessions2 <- purrr::map_dfr(
    seq(start_date, end_date, by = "day"),
    function(date) {
      message("Fetching data from ", date)
      c(year, month, day) %<-% wmf::extract_ymd(date)
      query <- glue(se_session_query2, .open = "${")
      result <- wmf::query_hive(query)
      result$date <- date
      return(result)
    }
  )
  message("Saving data")
  readr::write_csv(suggested_edit_sessions2, file.path(data_dir, "suggested_edit_sessions2.csv"))
  message("Compressing data")
  system(glue("gzip {data_dir}/suggested_edit_sessions2.csv"))
} else {
  suggested_edit_sessions2 <- readr::read_csv(file.path(data_dir, "suggested_edit_sessions2.csv.gz"), col_types = "cciD")
}

se_session_length <- suggested_edit_sessions2 %>%
  group_by(install_id) %>%
  summarize(avg_session_length = mean(session_length)) %>%
  summarize(avg_session_length = mean(avg_session_length)) %>%
  pull(avg_session_length) %>%
  round %>%
  lubridate::seconds_to_period() %>%
  tolower
