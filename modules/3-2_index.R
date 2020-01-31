all_reports <- dplyr::tibble(filename = dir("output", "[0-9]{4}-[0-9]{2}\\.html")) %>%
  mutate(
    name = sub(".html", "", filename, fixed = TRUE),
    year = sub("([0-9]{4})-([0-9]{2})", "\\1", name),
    month = month.name[as.numeric(sub("([0-9]{4})-([0-9]{2})", "\\2", name))],
    link = paste0("<a href=\"", filename, "\">", month, " ", year, "</a>")
  ) %>%
  arrange(dplyr::desc(name)) %>%
  select(link)

message("Index of reports to be saved as HTML:")
print(all_reports)

all_reports %>%
  knitr::kable(
    format = "html",
    col.names = c("Month"),
    align = c("l"),
    escape = FALSE,
    caption = "Source/documentation <a href=\"https://github.com/bearloga/wmf-android-monthly-report\">here</a>"
  ) %>%
  kableExtra::kable_styling(
    bootstrap_options = c("striped", "hover"),
    full_width = FALSE,
    font_size = 14
  ) %>%
  kableExtra::column_spec(1, width = "300px") %>%
  kableExtra::save_kable(
    file = file.path("output", "index.html"),
    self_contained = TRUE,
    bs_theme = "united"
  )
