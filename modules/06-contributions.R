# Output:
# - editor_data
# - suggested_editors_daily
# - suggested_editors_monthly
# - suggested_edits_daily
# - suggested_edits_monthly
# - inapp_edits_daily
# - inapp_edits_monthly

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
  rm(query_parameters, fetch_editor_stats) # quick cleanup
  readr::write_csv(editor_data, file.path(data_dir, "editor_data.csv"))
} else {
  editor_data <- readr::read_csv(file.path(data_dir, "editor_data.csv"), col_types = "ccDii")
}

suggested_editors_daily <- editor_data %>%
  keep_where(suggested_edits > 0, lubridate::year(date) == year, lubridate::month(date) == month) %>%
  group_by(date) %>%
  summarize(n_users = length(unique(user_name))) %>%
  summarize(avg_users = mean(n_users)) %>%
  pull(avg_users) %>%
  ceiling

suggested_editors_monthly <- editor_data %>%
  keep_where(suggested_edits > 0, lubridate::year(date) == year, lubridate::month(date) == month) %>%
  summarize(n_users = length(unique(user_name))) %>%
  pull(n_users)

suggested_edits_daily <- editor_data %>%
  keep_where(edit_type == "title description") %>%
  group_by(date) %>%
  summarize(suggested_edits = sum(suggested_edits)) %>%
  summarize(avg_suggested_edits = mean(suggested_edits)) %>%
  pull(avg_suggested_edits) %>%
  ceiling

suggested_edits_monthly <- editor_data %>%
  keep_where(edit_type == "title description") %>%
  summarize(suggested_edits = sum(suggested_edits)) %>%
  pull(suggested_edits)

inapp_edits_daily <- editor_data %>%
  keep_where(edit_type == "title description") %>%
  group_by(date) %>%
  summarize(total_edits = sum(total_edits)) %>%
  summarize(avg_total_edits = mean(total_edits)) %>%
  pull(avg_total_edits) %>%
  ceiling

inapp_edits_monthly <- editor_data %>%
  keep_where(edit_type == "title description") %>%
  summarize(total_edits = sum(total_edits)) %>%
  pull(total_edits)
