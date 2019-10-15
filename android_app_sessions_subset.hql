USE bearloga;
DROP TABLE IF EXISTS `android_app_sessions_subset`;

CREATE EXTERNAL TABLE IF NOT EXISTS `android_app_sessions_subset` (
    app_install_id       string COMMENT 'Wikipedia Android app install ID',
    languages            string COMMENT 'Languages set during this session',
    n_total_sessions     int    COMMENT 'Number of sessions the user had that day',
    n_reading_sessions   int    COMMENT 'Number of sessions the user had where they read at least 1 page',
    total_session_length bigint COMMENT 'Total time spent in app',
    total_reading_length bigint COMMENT 'Total time spent reading in app',
    total_pages_read     int    COMMENT 'Total number of pages read in app'
)
PARTITIONED BY (
    `day` int COMMENT 'Unpadded day of month'
)
STORED AS PARQUET
LOCATION 'hdfs://analytics-hadoop/tmp/android_app_sessions_subset';
