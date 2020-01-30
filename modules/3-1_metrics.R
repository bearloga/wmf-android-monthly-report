metric_labels <- c(
  "Total number of unique users",
  "Average session time per user",
  "Total number of unique users that have set multiple languages in the app",
  "Total number of unique users that have been logged in to Wikipedia in the app",
  "Total number of unique users who were at least on one article page",
  "Average session time of users on an article page",
  "Total number of unique users that have made an edit",
  "Average session time of users that made an edit",
  "Total number of unique users that have made an edit in “Suggested edits”",
  "Average session time of ”Suggested edits” users",
  "Median time spent in “Suggested Edits”",
  "Total number of unique users that have unlocked at least one “Suggested edits” feature",
  "Micro-contributions made in the app (add, translate, edit)",
  "Micro-contributions made in the app via Suggested edits (add, translate)",
  "Not-reverted image caption translations made via Suggested Edits",
  "Total number of unique users in Explore feed",
  "Average time spent in Explore feed"
)

metrics <- data.frame(
  daily_average = c(
    prettyNum(ceiling(mean(dau$n_users)), big.mark = " "),
    NA,
    NA,
    NA,
    readers_daily,
    NA,
    contributors_daily,
    NA,
    suggested_editors_daily,
    NA,
    NA,
    NA,
    sprintf(
      "%s (%s descriptions/day, %s captions/day)",
      prettyNum(inapp_edits_daily$`both`, big.mark = " "),
      prettyNum(inapp_edits_daily$`title description`, big.mark = " "),
      prettyNum(inapp_edits_daily$`image caption`, big.mark = " ")
    ),
    sprintf(
      "%s (%s/d descriptions/day, %s captions/day)",
      prettyNum(suggested_edits_daily$`both`, big.mark = " "),
      prettyNum(suggested_edits_daily$`title description`, big.mark = " "),
      prettyNum(suggested_edits_daily$`image caption`, big.mark = " ")
    ),
    NA,
    prettyNum(ceiling(mean(explore_feed_daily$n_users)), big.mark = " "),
    NA
  ),
  monthly_total = c(
    prettyNum(mau[1], big.mark = " "),
    NA,
    NA,
    NA,
    readers_monthly,
    NA,
    prettyNum(contributors_monthly, big.mark = " "),
    NA,
    prettyNum(suggested_editors_monthly, big.mark = " "),
    NA,
    NA,
    NA,
    sprintf(
      "%s (%s title descriptions, %s image captions)",
      prettyNum(inapp_edits_monthly$`title description` + inapp_edits_monthly$`image caption`, big.mark = " "),
      prettyNum(inapp_edits_monthly$`title description`, big.mark = " "),
      prettyNum(inapp_edits_monthly$`image caption`, big.mark = " ")
    ),
    sprintf(
      "%s (%s title descriptions, %s image captions)",
      prettyNum(suggested_edits_monthly$`title description` + suggested_edits_monthly$`image caption`, big.mark = " "),
      prettyNum(suggested_edits_monthly$`title description`, big.mark = " "),
      prettyNum(suggested_edits_monthly$`image caption`, big.mark = " ")
    ),
    sprintf(
      "%s cumulatively (%s just this month)",
      prettyNum(image_caption_translations$n_cumulative_total, big.mark = " "),
      prettyNum(image_caption_translations$n_new_this_month, big.mark = " ")
    ),
    image_caption_translations,
    prettyNum(explore_feed_monthly$n_users[1], big.mark = " "),
    NA
  ),
  value = c(
    NA,
    avg_session_time,
    multilang_stat,
    loggedin_stat,
    NA,
    tolower(lubridate::seconds_to_period(round(mean(reading_time$avg_reading_time)))),
    NA,
    contributors_session_length,
    NA,
    se_session_length,
    se_time_spent,
    unlock_stat,
    NA,
    NA,
    NA,
    NA,
    tolower(lubridate::seconds_to_period(round(mean(explore_feed_daily$avg_time_spent))))
  ),
  source = c(
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "in-app analytics",
    "mediawiki content db",
    "in-app analytics",
    "in-app analytics",
    "Suggested Edits backend db",
    "mediawiki content db",
    "mediawiki content db",
    "mediawiki content db",
    "in-app analytics",
    "in-app analytics"
  ),
  stringsAsFactors = FALSE
) %>%
  dplyr::mutate_all(~ ifelse(is.na(.x), "-", .x)) %>%
  set_rownames(metric_labels)

message("Table of metrics to be saved as HTML:")
print(metrics)

index <- "<a href=\"index.html\" title=\"Index of monthly metric reports\">⬅</a>"
metrics %>%
  knitr::kable(
    format = "html",
    caption = glue("{index} Wikipedia Android app metrics for {month.name[month]} {year}"),
    col.names = c("Daily average", "Monthly total", "Value", "Source"),
    align = c("r", "r", "r", "l"),
    escape = FALSE
  ) %>%
  kableExtra::kable_styling(
    bootstrap_options = c("striped", "hover"),
    full_width = FALSE,
    font_size = 14
  ) %>%
  kableExtra::save_kable(
    file = file.path("output", glue("{yyyy_mm}.html")),
    self_contained = TRUE,
    bs_theme = "united"
  )
