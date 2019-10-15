# Output:
# - suggested_edit_unlock
# - unlock_stat

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
} else {
  suggested_edit_unlock <- readr::read_csv(file.path(data_dir, "suggested_edit_unlock.csv"), col_types = "iTlD")
}

unlock_stat <- suggested_edit_unlock %>%
  count(unlocked) %>%
  spread(unlocked, n) %>%
  summarize(`TRUE` = mean(`TRUE`), `FALSE` = mean(`FALSE`)) %>%
  mutate(prop = `TRUE` / (`TRUE` + `FALSE`)) %>%
  mutate(summarized = sprintf("%.1f%% (%s out of %s)", 100 * prop, compress(`TRUE`), compress(`TRUE` + `FALSE`))) %>%
  pull(summarized)
