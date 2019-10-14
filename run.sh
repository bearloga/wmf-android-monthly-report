#!/bin/bash
cd /home/bearloga/android/metric_baselines/robin
{
  date
  echo "Getting data"
  Rscript data.R
  echo "Generating report"
  /home/bearloga/venv/bin/jupyter nbconvert --ExecutePreprocessor.timeout=900 --execute --to notebook android-app-stats.ipynb
  cp output/*.html /srv/published-datasets/wikipedia-android-app-reports/metrics/
} >> /home/bearloga/android/metric_baselines/robin/run.log 2>&1
