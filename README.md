# Monthly Android report card

This repository has all the scripts and queries necessary to produce the [monthly reports](https://analytics.wikimedia.org/published/datasets/wikipedia-android-app-reports/metrics/) for [Wikipedia Android app](https://www.mediawiki.org/wiki/Wikimedia_Apps/Team/Android).

## Setup and dependencies

In R, use the following commands to install the required packages:

```R
install.packages(c(
  "import", "zeallot", "remotes", "fs", "glue",
  "tidyverse", "knitr", "kableExtra"
))
remotes::install_github("wikimedia/wikimedia-discovery-wmf")
remotes::install_github("wikimedia/wikimedia-discovery-polloi")
```

## Structure and flow

The scripts should only be run **after** the latest `mediawiki_history` snapshot is ready. Those are usually ready within the first couple days of each month, so don't schedule these scripts to run on the first day of each month.

If [Kerberos authentication via keytab](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Kerberos/UserGuide#Run_a_recurrent_job_via_Cron_or_similar_without_kinit_every_day) is available, [run.sh](run.sh) can be scheduled via crontab. Scheduling a new report card to be generated on the 7th day of every month would look like:

```
0 0 7 * * bash /home/bearloga/android/metric_baselines/monthly_report/run.sh
```

- [data.R](data.R) extracts, aggregates, and loads a subset of the [MobileWikiAppSessions](https://meta.wikimedia.org/wiki/Schema:MobileWikiAppSessions) EventLogging data into `bearloga.android_app_sessions_subset` in Hive.
  - This makes some of the metrics much easier and faster to calculate.
  - This is the very first script that is run.
- [report.R](report.R) is the script that executes the various modules in order.
  - Saves progress to a file between each one, so that if any of the modules experience problems the script can be resumed from that point without having to re-run the previous modules.

The modules (sub-scripts run by report.R) are as follows:

- **Section 1: Initialize**
  1. [setup.R](modules/1-1_setup.R) attaches libraries, determines all the dates to use in queries, and sets up the paths that data and output will be written to
- **Section 2: Get Metrics**
  - modules 2-1 through 2-9 are for calculating the various metrics that show up in the report card
- **Section 3: Finalize**
  1. [metrics.R](modules/3-1_metrics.R) puts together the report card and writes it out as a web page in the [output](output/) directory.
  2. [index.R](modules/3-2_index.R) generates a new index page of the monthly report cards, which also goes in the output/ directory.
  3. [archive.R](modules/3-3_archive.R) compresses, archives, and moves the generated data files to Hadoop Distributed File System (HDFS) for long-term storage.
