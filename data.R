library(glue)
library(zeallot)
library(magrittr)
import::from(dplyr, group_by, summarize, ungroup, mutate, select, arrange, keep_where = filter, rename, count, pull)
import::from(tidyr, spread, gather)

# Intelligently figure out the year & month to calculate stats for:
today <- lubridate::today()
beginning_of_curr_month <- lubridate::floor_date(today, "month")
last_day_of_prev_month <- beginning_of_curr_month - 1
year <- lubridate::year(last_day_of_prev_month)
month <- lubridate::month(last_day_of_prev_month)

# Calculate date range based on year & month given above:
start_date <- as.Date(paste(year, month, "1", sep = "-"), format = "%Y-%m-%d")
end_date <- start_date + months(1) - 1
# Set up organization for data:
yyyy_mm <- sprintf("%.0f-%02.0f", year, month)
data_dir <- file.path("data", yyyy_mm)
fs::dir_create(data_dir)

message(glue("Data will be fetched for {month.name[month]} {year} (from {start_date} to {end_date})"))

# Daily stats:
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
    rm(daily_stats); gc()
}

# Sessions:
if (!file.exists(file.path(data_dir, "session_summaries.csv.gz"))) {
    session_query <- "WITH unique_sessions AS (
      SELECT DISTINCT
        event.app_install_id,
        event.session_token,
        event.languages,
        event.length AS session_length,
        event.totalpages AS total_articles
      FROM event.MobileWikiAppSessions
      WHERE year = ${year} AND month = ${month} AND day = ${day}
        AND revision = 18948969
        AND INSTR(useragent.wmf_app_version, '-r-') > 0
    )
    SELECT
      app_install_id,
      MAX(languages) AS languages,
      SUM(IF(session_length >= 0, 1, 0)) AS n_total_sessions,
      SUM(IF(session_length >= 0 AND total_articles > 0, 1, 0)) AS n_reading_sessions,
      SUM(IF(session_length >= 0, session_length, 0)) AS total_session_length,
      SUM(IF(session_length >= 0 AND total_articles > 0, session_length, 0)) AS total_reading_length
    FROM unique_sessions
    GROUP BY app_install_id
    HAVING n_total_sessions > 0"
    message("Fetching session data")
    session_summaries <- purrr::map_dfr(
        seq(start_date, end_date, by = "day"),
        function(date) {
            message("Fetching data from ", date)
            c(year, month, day) %<-% wmf::extract_ymd(date)
            query <- glue(session_query, .open = "${")
            result <- wmf::query_hive(query)
            result$n_languages <- nchar(result$languages) - nchar(gsub(",", "", result$languages, fixed = TRUE)) + 1
            result$languages <- NULL
            result$date <- date
            return(result)
        }
    )
    message("Saving session summaries")
    readr::write_csv(session_summaries, file.path(data_dir, "session_summaries.csv"))
    rm(session_summaries); gc()
    message("Compressing session summaries")
    system(glue("gzip {data_dir}/session_summaries.csv"))
}

# Reading:
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
    rm(reading_time); gc()
}

# Edits:
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
    rm(edit_session_summaries); gc()
}

# Suggested Edits:
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
    rm(suggested_edit_sessions); gc()
}

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
    rm(suggested_edit_sessions2); gc()
    message("Compressing data")
    system(glue("gzip {data_dir}/suggested_edit_sessions2.csv"))
}

# Suggested Edits contributions:
if (!file.exists(file.path(data_dir, "editor_data.csv"))) {
    editor_query <- "SELECT
      actor_name AS user_name,
      SUBSTR(rev_timestamp, 1, 8) AS `date`,
      SUM(IF(INSTR(rev_comment, '#suggestededit') > 0 OR INSTR(comment_text, '#suggestededit') > 0, 1, 0)) AS suggested_edits,
      COUNT(1) AS total_edits
    FROM revision
    LEFT JOIN revision_comment_temp rct ON revision.rev_id = rct.revcomment_rev
    LEFT JOIN `comment` ON rct.revcomment_comment_id = `comment`.comment_id
    LEFT JOIN change_tag ON revision.rev_id = change_tag.ct_rev_id
    LEFT JOIN revision_actor_temp rat ON revision.rev_id = rat.revactor_rev
    LEFT JOIN actor ON rat.revactor_actor = actor.actor_id
    ${extra_join}
    WHERE rev_timestamp >= '${rev_timestamp}'
      AND actor_user IS NOT NULL
      AND rat.revactor_actor > 0 -- remove anon edits (T188327 & T215466)
      AND ct_tag_id = ${change_tag} -- android app edit
      ${extra_condition}
      AND rev_timestamp <= '${format(end_date, '%Y%m%d')}' -- up to end_date
    GROUP BY user_name, `date`"
    query_parameters <- list(
        wiki_db = c("title description" = "wikidatawiki", "image caption" = "commonswiki"),
        rev_timestamp = c("20190401", "20190601"),
        change_tag = c(14, 22),
        extra_join = c("", "LEFT JOIN page ON revision.rev_page = page.page_id"),
        extra_condition = c("", "AND INSTR(comment_text, '* wbsetlabel-') > 0 AND page_namespace = 6")
    )

    fetch_editor_stats <- function(wiki_db, rev_timestamp, change_tag, extra_join, extra_condition) {
        editor_query <- glue(editor_query, .open = "${")
        editor_stats <- wmf::mysql_read(editor_query, wiki_db)
        return(editor_stats)
    }
    message("Fetching app microcontribution stats")
    editor_data <- purrr::pmap_dfr(query_parameters, fetch_editor_stats, .id = "edit_type") %>%
        mutate(date = as.Date(date, "%Y%m%d"))
    readr::write_csv(editor_data, file.path(data_dir, "editor_data.csv"))
    rm(editor_data); gc()
}

# Suggested Edits unlock stats:
if (!file.exists(file.path(data_dir, "suggested_edit_unlock.csv"))) {
    unlock_query <- "SELECT
      counts.wetc_user AS user_id,
      MIN(unlocks.wettp_effective_time) AS unlock_ts
    FROM wikimedia_editor_tasks_counts counts
    LEFT JOIN wikimedia_editor_tasks_targets_passed unlocks ON (
      counts.wetc_user = unlocks.wettp_user
      AND counts.wetc_key_id = unlocks.wettp_key_id
    )
    GROUP BY user_id"
    message("Fetching Suggested Edits unlock stats")
    suggested_edit_unlock <- wmf::mysql_read(unlock_query, database = "wikishared", use_x1 = TRUE) %>%
        mutate(
            unlock_ts = lubridate::ymd_hms(unlock_ts),
            unlocked = !is.na(unlock_ts),
            date = as.Date(unlock_ts)
        ) %>%
        keep_where(is.na(date) | date <= end_date)
    readr::write_csv(suggested_edit_unlock, file.path(data_dir, "suggested_edit_unlock.csv"))
    rm(suggested_edit_unlock); gc()
}

# Feed stats:
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
    rm(explore_feed_monthly); gc()
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
    rm(explore_feed_daily); gc()
}
