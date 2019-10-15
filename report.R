progress_savefile <- "progress.RData"

run_module <- function(path) {
  message("Executing ", path)
  source(path)
  message("Saving progress to ", progress_savefile)
  save.image(progress_savefile, compress = TRUE)
}

modules <- dir("modules", pattern = "\\.R$", full.names = TRUE)

purrr::walk(modules, run_module)
