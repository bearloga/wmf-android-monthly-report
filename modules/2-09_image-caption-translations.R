# Output:
# - image_caption_translations

if (!file.exists(file.path(data_dir, "image_caption_translations.csv"))) {
  ict_query <- "USE wmf;
SELECT
  COUNT(1) AS n_cumulative_total,
  SUM(IF(event_timestamp >= '${yyyy_mm}-01', 1, 0)) AS n_new_this_month
FROM mediawiki_history
WHERE snapshot = '${yyyy_mm}'
  AND wiki_db = 'commonswiki'
  AND event_entity = 'revision'
  AND ARRAY_CONTAINS(revision_tags, 'android app edit')
  AND INSTR(event_comment, '#suggestededit-translate') > 0
  -- Revisions that haven't been reverted as of the snapshot:
  AND NOT revision_is_identity_reverted
  AND NOT revision_is_deleted_by_page_deletion;"
  image_caption_translations <- wmf::query_hive(glue(ict_query, .open = "${"))
  readr::write_csv(image_caption_translations, file.path(data_dir, "image_caption_translations.csv"))
} else {
  message("Loading image caption translation count")
  image_caption_translations <- readr::read_csv(file.path(data_dir, "image_caption_translations.csv"), col_types = "i")
}
