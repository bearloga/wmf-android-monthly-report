#!/bin/bash
cd /home/bearloga/android/metric_baselines/robin
{
  date
  echo "Extracting a subset of session data"
  Rscript data.R
  echo "Generating a report"
  Rscript report.R
  cp output/*.html /srv/published-datasets/wikipedia-android-app-reports/metrics/
} >> /home/bearloga/android/metric_baselines/robin/run.log 2>&1
