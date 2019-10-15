# Output:
# - dau
# - mau
# - avg_session_time

mau_query <- "USE bearloga;
SELECT COUNT(DISTINCT app_install_id) AS n_users
FROM android_app_sessions_subset
WHERE day >= 1"
mau <- wmf::query_hive(mau_query)$n_users

dau_query <- "USE bearloga;
SELECT day, COUNT(DISTINCT app_install_id) AS n_users
FROM android_app_sessions_subset
WHERE day >= 1
GROUP BY day"
dau <- wmf::query_hive(dau_query)

session_time_query <- "USE bearloga;
SELECT AVG(total_session_length / n_total_sessions) AS avg_session_time
FROM android_app_sessions_subset
WHERE day >= 1"
avg_session_time <- wmf::query_hive(session_time_query)$avg_session_time[1] %>%
  round %>%
  lubridate::seconds_to_period() %>%
  tolower
