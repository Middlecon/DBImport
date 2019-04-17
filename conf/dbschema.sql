/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

-- Dumping structure for tabell DBImport.airflow_custom_dags
CREATE TABLE IF NOT EXISTS `airflow_custom_dags` (
  `dag_name` varchar(64) NOT NULL COMMENT 'Name of Airflow DAG.',
  `schedule_interval` varchar(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag',
  `retries` int(11) NOT NULL DEFAULT '0',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  `application_notes` text COMMENT 'Free text field that can be used for application documentaton, notes or links. ',
  PRIMARY KEY (`dag_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_dag_sensors
CREATE TABLE IF NOT EXISTS `airflow_dag_sensors` (
  `dag_name` varchar(64) NOT NULL COMMENT 'Name of DAG to add sensor to',
  `sensor_name` varchar(64) NOT NULL COMMENT 'name of the sensor Task',
  `wait_for_dag` varchar(64) NOT NULL COMMENT 'Name of DAG to wait for. Must be defined in DBImport',
  `wait_for_task` varchar(64) DEFAULT NULL COMMENT 'Name of task to wait for.Default is ''stop''',
  `timeout_minutes` int(11) DEFAULT NULL COMMENT 'Number of minutes to wait for DAG.',
  PRIMARY KEY (`dag_name`,`sensor_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for view DBImport.airflow_dag_triggers
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `airflow_dag_triggers` (
	`dag_name` VARCHAR(64) NOT NULL COLLATE 'latin1_swedish_ci',
	`trigger_name` TEXT NULL COLLATE 'latin1_swedish_ci',
	`dag_to_trigger` TEXT NOT NULL COLLATE 'latin1_swedish_ci'
) ENGINE=MyISAM;

-- Dumping structure for tabell DBImport.airflow_etl_dags
CREATE TABLE IF NOT EXISTS `airflow_etl_dags` (
  `dag_name` varchar(64) NOT NULL COMMENT 'Name of Airflow DAG.',
  `schedule_interval` varchar(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag',
  `filter_job` varchar(64) NOT NULL,
  `filter_task` varchar(64) DEFAULT NULL,
  `filter_source_db` varchar(256) DEFAULT NULL,
  `filter_target_db` varchar(256) DEFAULT NULL,
  `retries` tinyint(4) DEFAULT NULL,
  `trigger_dag_on_success` varchar(64) DEFAULT NULL,
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  `application_notes` text COMMENT 'Free text field that can be used for application documentaton, notes or links. ',
  PRIMARY KEY (`dag_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_execution_type
CREATE TABLE IF NOT EXISTS `airflow_execution_type` (
  `executionid` int(11) NOT NULL,
  `execution_type` text NOT NULL,
  PRIMARY KEY (`executionid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_export_dags
CREATE TABLE IF NOT EXISTS `airflow_export_dags` (
  `dag_name` varchar(64) NOT NULL COMMENT 'Name of Airflow DAG.',
  `schedule_interval` varchar(20) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag',
  `filter_dbalias` varchar(256) NOT NULL COMMENT 'Filter string for DBALIAS in export_tables',
  `filter_target_schema` varchar(256) DEFAULT NULL COMMENT 'Filter string for TARGET_SCHEMA  in export_tables',
  `filter_target_table` varchar(256) DEFAULT NULL COMMENT 'Filter string for TARGET_TABLE  in export_tables',
  `retries` tinyint(4) DEFAULT NULL,
  `trigger_dag_on_success` varchar(64) DEFAULT NULL COMMENT 'Name of DAG to trigger if export is successfull. Comma seperated list of DAGs (optional)',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  `application_notes` text COMMENT 'Free text field that can be used for application documentaton, notes or links. ',
  PRIMARY KEY (`dag_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_import_dags
CREATE TABLE IF NOT EXISTS `airflow_import_dags` (
  `dag_name` varchar(64) NOT NULL COMMENT 'Name of Airflow DAG.',
  `schedule_interval` varchar(32) NOT NULL DEFAULT 'None' COMMENT 'Time to execute dag',
  `filter_hive` varchar(256) NOT NULL COMMENT 'Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE',
  `filter_hive_db` varchar(256) DEFAULT NULL COMMENT 'NOT USED: Filter string for HIVE_DB in import_tables',
  `filter_hive_table` varchar(256) DEFAULT NULL COMMENT 'NOT USED: Filter string for HIVE_TABLE in import_tables',
  `finish_all_stage1_first` tinyint(4) NOT NULL DEFAULT '0',
  `retries` tinyint(4) NOT NULL DEFAULT '5',
  `retries_stage1` tinyint(4) DEFAULT NULL,
  `retries_stage2` tinyint(4) DEFAULT NULL,
  `pool_stage1` varchar(256) DEFAULT NULL COMMENT 'Airflow pool used for stage1 tasks. NULL for default pool',
  `pool_stage2` varchar(256) DEFAULT NULL COMMENT 'Airflow pool used for stage2 tasks. NULL for default pool',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  `application_notes` text COMMENT 'Free text field that can be used for application documentaton, notes or links. ',
  `auto_table_discovery` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`dag_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_import_dag_execution
CREATE TABLE IF NOT EXISTS `airflow_import_dag_execution` (
  `dag_name` varchar(64) NOT NULL,
  `executionid` int(11) NOT NULL,
  `task_config` text NOT NULL,
  `task_name` text,
  KEY `FK_airflow_import_dag_execution_airflow_execution_type` (`executionid`),
  CONSTRAINT `FK_airflow_import_dag_execution_airflow_execution_type` FOREIGN KEY (`executionid`) REFERENCES `airflow_execution_type` (`executionid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_import_task_execution
CREATE TABLE IF NOT EXISTS `airflow_import_task_execution` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `stage` tinyint(4) NOT NULL,
  `executionid` int(11) NOT NULL,
  `task_config` text NOT NULL,
  `task_name` text,
  PRIMARY KEY (`hive_db`,`hive_table`,`stage`),
  KEY `FK_airflow_import_task_execution_airflow_execution_type` (`executionid`),
  CONSTRAINT `FK_airflow_import_task_execution_airflow_execution_type` FOREIGN KEY (`executionid`) REFERENCES `airflow_execution_type` (`executionid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.airflow_tasks
CREATE TABLE IF NOT EXISTS `airflow_tasks` (
  `dag_name` varchar(64) NOT NULL,
  `task_name` varchar(64) NOT NULL,
  `task_type` enum('shell script','Hive SQL Script','JDBC SQL') NOT NULL DEFAULT 'Hive SQL Script',
  `placement` enum('before main','after main','in main') NOT NULL DEFAULT 'after main',
  `jdbc_dbalias` varchar(256) DEFAULT NULL COMMENT 'For  ''JDBC SQL'' Task Type, this specifies what database the SQL should run against',
  `hive_db` varchar(256) DEFAULT NULL,
  `airflow_pool` varchar(64) DEFAULT NULL,
  `airflow_priority` tinyint(4) DEFAULT NULL,
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1',
  `task_dependency_in_main` varchar(256) DEFAULT NULL,
  `task_config` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`dag_name`,`task_name`),
  KEY `FK_airflow_tasks_airflow_execution_type` (`task_type`),
  KEY `FK_airflow_tasks_jdbc_connections` (`jdbc_dbalias`),
  CONSTRAINT `FK_airflow_tasks_jdbc_connections` FOREIGN KEY (`jdbc_dbalias`) REFERENCES `jdbc_connections` (`dbalias`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.auto_discovered_tables
CREATE TABLE IF NOT EXISTS `auto_discovered_tables` (
  `hive_db` varchar(256) NOT NULL,
  `dbalias` varchar(256) NOT NULL,
  `source_schema` varchar(256) NOT NULL,
  `source_table` varchar(256) NOT NULL,
  `discovery_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `migrate_to_import_tables` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`hive_db`,`dbalias`,`source_schema`,`source_table`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.etl_jobs
CREATE TABLE IF NOT EXISTS `etl_jobs` (
  `job` varchar(64) NOT NULL,
  `task` varchar(64) NOT NULL,
  `job_id` int(11) NOT NULL AUTO_INCREMENT,
  `etl_type` varchar(32) NOT NULL DEFAULT '0',
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1',
  `source_db` varchar(256) DEFAULT NULL,
  `source_table` varchar(256) DEFAULT NULL,
  `target_db` varchar(256) DEFAULT NULL,
  `target_table` varchar(256) DEFAULT NULL,
  `operator_notes` varchar(256) DEFAULT NULL COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`job`,`task`),
  UNIQUE KEY `job_id` (`job_id`)
) ENGINE=InnoDB AUTO_INCREMENT=125 DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.export_columns
CREATE TABLE IF NOT EXISTS `export_columns` (
  `table_id` int(11) unsigned NOT NULL,
  `column_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `column_name` varchar(256) NOT NULL,
  `column_order` int(11) DEFAULT NULL,
  `hive_db` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `hive_table` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `target_column_name` varchar(256) DEFAULT NULL,
  `last_update_from_hive` datetime NOT NULL,
  `last_export_time` datetime DEFAULT NULL,
  `selection` varchar(256) DEFAULT NULL,
  `include_in_export` tinyint(4) NOT NULL DEFAULT '1',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`table_id`,`column_name`,`column_id`),
  UNIQUE KEY `column_id` (`column_id`),
  CONSTRAINT `FK_export_columns_export_tables` FOREIGN KEY (`table_id`) REFERENCES `export_tables` (`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=195792 DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.export_tables
CREATE TABLE IF NOT EXISTS `export_tables` (
  `dbalias` varchar(256) NOT NULL,
  `target_schema` varchar(256) NOT NULL,
  `target_table` varchar(256) NOT NULL,
  `table_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `export_type` varchar(16) NOT NULL DEFAULT 'full' COMMENT 'full or incr',
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `last_update_from_hive` datetime DEFAULT NULL,
  `sql_where_addition` varchar(1024) DEFAULT NULL COMMENT 'Will be added AFTER the SQL WHERE. If it''s an incr export, this will be after the incr limit statements. Example "orderId > 1000"',
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Will the table be included in Airflow DAG when it matches the DAG selection',
  `export_history` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'If set to 1, the DBImport definition is used for the hive_table without ''_history'' at the end of the tablename',
  `source_is_view` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Auto updated by ''update_DB_export_from_hive.sh''',
  `source_is_acid` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Auto updated by ''update_DB_export_from_hive.sh''',
  `validate_export` tinyint(4) NOT NULL DEFAULT '1',
  `uppercase_columns` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto (Oracle = uppercase, other databases = lowercase)',
  `truncate_target` tinyint(4) NOT NULL DEFAULT '1',
  `mappers` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto, 0 = invalid. Auto updated by ''export_main.sh''',
  `hive_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `target_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `incr_column` varchar(256) DEFAULT NULL,
  `incr_minvalue` varchar(25) DEFAULT NULL,
  `incr_maxvalue` varchar(25) DEFAULT NULL,
  `incr_minvalue_pending` varchar(25) DEFAULT NULL,
  `incr_maxvalue_pending` varchar(25) DEFAULT NULL,
  `sqoop_options` varchar(1024) DEFAULT NULL COMMENT 'Auto updated when target table is created.',
  `sqoop_last_size` bigint(20) DEFAULT NULL,
  `sqoop_last_rows` bigint(20) DEFAULT NULL,
  `create_target_table_sql` text COMMENT 'Auto updated when target table is created.',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`dbalias`,`target_schema`,`target_table`),
  UNIQUE KEY `table_id` (`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6981237 DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_columns
CREATE TABLE IF NOT EXISTS `import_columns` (
  `table_id` int(10) unsigned NOT NULL,
  `column_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `column_order` int(10) unsigned DEFAULT NULL,
  `column_name` varchar(256) NOT NULL,
  `hive_db` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `hive_table` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `source_column_name` varchar(256) NOT NULL,
  `column_type` varchar(2048) NOT NULL COMMENT 'Updated by get_source_tableschema.sh',
  `source_column_type` varchar(2048) NOT NULL COMMENT 'Updated by get_source_tableschema.sh',
  `source_database_type` varchar(256) DEFAULT NULL COMMENT 'Updated by get_source_tableschema.sh',
  `sqoop_column_type` varchar(256) DEFAULT NULL COMMENT 'Used to create a correct --map-column-java setting for sqoop. Updated by get_source_tableschema.sh',
  `force_string` tinyint(4) NOT NULL DEFAULT '-1' COMMENT 'If set to 1, this forces char and varchars to be string in Hive',
  `include_in_import` tinyint(4) NOT NULL DEFAULT '1',
  `source_primary_key` tinyint(4) DEFAULT NULL COMMENT 'Number starting from 1 listing the order of the column in the PK',
  `last_update_from_source` datetime NOT NULL,
  `comment` text,
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`table_id`,`column_id`,`column_name`),
  UNIQUE KEY `column_id` (`column_id`),
  KEY `table_id_source_column_name` (`table_id`,`source_column_name`),
  CONSTRAINT `FK__import_tables` FOREIGN KEY (`table_id`) REFERENCES `import_tables` (`table_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=867342 DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_failure_log
CREATE TABLE IF NOT EXISTS `import_failure_log` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `eventtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `severity` text,
  `import_type` text,
  `error_text` text
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_foreign_keys
CREATE TABLE IF NOT EXISTS `import_foreign_keys` (
  `table_id` int(10) unsigned NOT NULL,
  `column_id` int(10) unsigned NOT NULL,
  `fk_index` int(10) unsigned NOT NULL,
  `fk_table_id` int(10) unsigned NOT NULL,
  `fk_column_id` int(10) unsigned NOT NULL,
  `key_position` int(10) unsigned NOT NULL,
  PRIMARY KEY (`table_id`,`column_id`,`fk_index`),
  KEY `FK_import_foreign_keys_import_columns_FK` (`fk_table_id`,`fk_column_id`),
  CONSTRAINT `FK_import_foreign_keys_import_columns` FOREIGN KEY (`table_id`, `column_id`) REFERENCES `import_columns` (`table_id`, `column_id`) ON DELETE CASCADE,
  CONSTRAINT `FK_import_foreign_keys_import_columns_FK` FOREIGN KEY (`fk_table_id`, `fk_column_id`) REFERENCES `import_columns` (`table_id`, `column_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for view DBImport.import_foreign_keys_VIEW
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `import_foreign_keys_VIEW` (
	`hive_db` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`hive_table` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`fk_index` INT(10) UNSIGNED NOT NULL,
	`column_name` VARCHAR(256) NOT NULL COLLATE 'latin1_swedish_ci',
	`ref_hive_Db` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`ref_hive_table` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`ref_column_name` VARCHAR(256) NOT NULL COLLATE 'latin1_swedish_ci'
) ENGINE=MyISAM;

-- Dumping structure for tabell DBImport.import_retries_log
CREATE TABLE IF NOT EXISTS `import_retries_log` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `retry_time` datetime NOT NULL,
  `stage` int(11) NOT NULL,
  `stage_description` text NOT NULL,
  `import_type` text,
  `unrecoverable_error` tinyint(4) DEFAULT NULL,
  `get_source_rowcount_start` datetime DEFAULT NULL,
  `get_source_rowcount_stop` datetime DEFAULT NULL,
  `get_source_rowcount_duration` bigint(20) DEFAULT NULL,
  `sqoop_start` datetime DEFAULT NULL,
  `sqoop_stop` datetime DEFAULT NULL,
  `sqoop_duration` bigint(20) DEFAULT NULL,
  `sqoop_mappers` bigint(20) DEFAULT NULL,
  `sqoop_rows` bigint(20) DEFAULT NULL,
  `sqoop_size` bigint(20) DEFAULT NULL,
  `source_table_rowcount` bigint(20) DEFAULT NULL,
  `target_table_rowcount` bigint(20) DEFAULT NULL,
  `incr_minvalue` text,
  `incr_maxvalue` text,
  `incr_column` text,
  `logdir` text,
  `timefile` text,
  PRIMARY KEY (`hive_db`,`hive_table`,`retry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_stage
CREATE TABLE IF NOT EXISTS `import_stage` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `stage` int(11) NOT NULL COMMENT 'Current stage of the import. This is an internal stage and has nothing to do with stage 1 and 2 of import_main',
  `stage_description` text NOT NULL,
  `import_type` text,
  `unrecoverable_error` tinyint(4) DEFAULT NULL,
  `get_source_rowcount_start` datetime DEFAULT NULL,
  `get_source_rowcount_stop` datetime DEFAULT NULL,
  `get_source_rowcount_duration` bigint(20) DEFAULT NULL,
  `sqoop_start` datetime DEFAULT NULL,
  `sqoop_stop` datetime DEFAULT NULL,
  `sqoop_duration` bigint(20) DEFAULT NULL,
  `sqoop_mappers` bigint(20) DEFAULT NULL,
  `sqoop_rows` bigint(20) DEFAULT NULL,
  `sqoop_size` bigint(20) DEFAULT NULL,
  `source_table_rowcount` bigint(20) DEFAULT NULL,
  `target_table_rowcount` bigint(20) DEFAULT NULL,
  `incr_minvalue` text,
  `incr_maxvalue` text,
  `incr_column` text,
  `logdir` text,
  `timefile` text,
  PRIMARY KEY (`hive_db`,`hive_table`,`stage`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_tables
CREATE TABLE IF NOT EXISTS `import_tables` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `table_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `dbalias` varchar(256) NOT NULL,
  `source_schema` varchar(256) NOT NULL,
  `source_table` varchar(256) NOT NULL,
  `import_type` varchar(32) NOT NULL DEFAULT 'full',
  `last_update_from_source` datetime DEFAULT NULL,
  `sqoop_sql_where_addition` varchar(1024) DEFAULT NULL COMMENT 'Will be added AFTER the SQL WHERE. If it''s an incr import, this will be after the incr limit statements. Example "orderId > 1000"',
  `nomerge_ingestion_sql_addition` varchar(2048) DEFAULT NULL COMMENT 'This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables',
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Will the table be included in Airflow DAG when it matches the DAG selection',
  `airflow_priority` tinyint(4) DEFAULT NULL COMMENT 'This will set priority_weight for both stage1 and stage2 in Airflow',
  `validate_import` tinyint(4) NOT NULL DEFAULT '1',
  `validate_diff_allowed` bigint(20) NOT NULL DEFAULT '-1' COMMENT '-1 = auto calculated diff allowed',
  `truncate_hive` tinyint(4) NOT NULL DEFAULT '1',
  `mappers` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto, 0 = invalid. Auto updated by ''export_main.sh''',
  `soft_delete_during_merge` tinyint(4) NOT NULL DEFAULT '0',
  `source_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `hive_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `incr_mode` varchar(16) DEFAULT NULL COMMENT 'append or lastmodified',
  `incr_column` varchar(256) DEFAULT NULL,
  `incr_minvalue` varchar(32) DEFAULT NULL,
  `incr_maxvalue` varchar(32) DEFAULT NULL,
  `incr_minvalue_pending` varchar(32) DEFAULT NULL,
  `incr_maxvalue_pending` varchar(32) DEFAULT NULL,
  `pk_column_override` varchar(1024) DEFAULT NULL,
  `pk_column_override_mergeonly` varchar(1024) DEFAULT NULL,
  `hive_merge_heap` int(11) DEFAULT NULL COMMENT 'Should be a multiple of Yarn container size. (3072 chunks)',
  `concatenate_hive_table` tinyint(4) NOT NULL DEFAULT '-1',
  `sqoop_query` text,
  `sqoop_options` text,
  `sqoop_last_size` bigint(20) DEFAULT NULL,
  `sqoop_last_rows` bigint(20) DEFAULT NULL,
  `sqoop_last_execution` bigint(20) DEFAULT NULL,
  `sqoop_use_generated_sql` tinyint(4) NOT NULL DEFAULT '-1',
  `sqoop_allow_text_splitter` tinyint(4) NOT NULL DEFAULT '0',
  `force_string` tinyint(4) NOT NULL DEFAULT '-1',
  `comment` text,
  `generated_hive_column_definition` text COMMENT 'Same as settings in TABLE_SPECIFICS GENERATED FILE',
  `generated_sqoop_query` text COMMENT 'Same as settings in TABLE_SPECIFICS GENERATED FILE',
  `generated_sqoop_options` text COMMENT 'Same as settings in TABLE_SPECIFICS GENERATED FILE',
  `generated_pk_columns` text COMMENT 'Same as settings in TABLE_SPECIFICS GENERATED FILE',
  `generated_foreign_keys` text COMMENT 'Same as settings in TABLE_SPECIFICS GENERATED FILE',
  `datalake_source` varchar(256) DEFAULT NULL COMMENT 'This value will come in the dbimport_source column if present. Priority is table, connection',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`hive_db`,`hive_table`),
  UNIQUE KEY `table_id` (`table_id`),
  KEY `hive_db_dbalias_source_schema_source_table` (`hive_db`,`dbalias`,`source_schema`,`source_table`)
) ENGINE=InnoDB AUTO_INCREMENT=8000026 DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.jdbc_connections
CREATE TABLE IF NOT EXISTS `jdbc_connections` (
  `dbalias` varchar(256) NOT NULL,
  `private_key_path` varchar(128) DEFAULT NULL,
  `public_key_path` varchar(128) DEFAULT NULL,
  `jdbc_url` text NOT NULL,
  `credentials` text,
  `datalake_source` varchar(256) DEFAULT NULL COMMENT 'This value will come in the dbimport_source column if present. Priority is table, connection',
  `force_string` tinyint(4) NOT NULL DEFAULT '-1',
  `create_datalake_import` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'If set to 1, the datalake_import column will be created on all tables that is using this dbalias',
  `timewindow_start` time DEFAULT NULL,
  `timewindow_stop` time DEFAULT NULL,
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`dbalias`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.json_to_rest
CREATE TABLE IF NOT EXISTS `json_to_rest` (
  `endpoint` text NOT NULL,
  `status` tinyint(4) NOT NULL,
  `jsondata` text NOT NULL,
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10422 DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
-- Dumping structure for view DBImport.airflow_dag_triggers
-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `airflow_dag_triggers`;
CREATE ALGORITHM=UNDEFINED DEFINER=`dbimport`@`%` SQL SECURITY DEFINER VIEW `airflow_dag_triggers` AS select `airflow_import_dag_execution`.`dag_name` AS `dag_name`,`airflow_import_dag_execution`.`task_name` AS `trigger_name`,`airflow_import_dag_execution`.`task_config` AS `dag_to_trigger` from `airflow_import_dag_execution`;

-- Dumping structure for view DBImport.import_foreign_keys_VIEW
-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `import_foreign_keys_VIEW`;
CREATE ALGORITHM=UNDEFINED DEFINER=`dbimport`@`%` SQL SECURITY DEFINER VIEW `import_foreign_keys_VIEW` AS select `s`.`hive_db` AS `hive_db`,`s`.`hive_table` AS `hive_table`,`fk`.`fk_index` AS `fk_index`,`s`.`column_name` AS `column_name`,`ref`.`hive_db` AS `ref_hive_Db`,`ref`.`hive_table` AS `ref_hive_table`,`ref`.`column_name` AS `ref_column_name` from ((`import_foreign_keys` `fk` join `import_columns` `s` on(((`s`.`table_id` = `fk`.`table_id`) and (`s`.`column_id` = `fk`.`column_id`)))) join `import_columns` `ref` on(((`ref`.`table_id` = `fk`.`fk_table_id`) and (`ref`.`column_id` = `fk`.`fk_column_id`)))) order by `fk`.`fk_index`,`fk`.`key_position`;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;

