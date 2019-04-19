-- --------------------------------------------------------
-- Värd:                         l4326pp.sss.se.scania.com
-- Serverversion:                5.7.19-enterprise-commercial-advanced - MySQL Enterprise Server - Advanced Edition (Commercial)
-- Server OS:                    Linux
-- HeidiSQL Version:             9.5.0.5196
-- --------------------------------------------------------

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
  `table_id` int(11) unsigned NOT NULL COMMENT 'Foreign Key to export_tables column ''table_id''',
  `column_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier',
  `column_name` varchar(256) NOT NULL COMMENT 'Name of column in target table. Dont change this manually',
  `column_order` int(11) DEFAULT NULL COMMENT 'The order of the columns',
  `hive_db` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `hive_table` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `target_column_name` varchar(256) DEFAULT NULL COMMENT 'Name of column in the target system',
  `last_update_from_hive` datetime NOT NULL COMMENT 'Timestamp of last schema update from Hive',
  `last_export_time` datetime DEFAULT NULL COMMENT 'Timestamp of last export',
  `selection` varchar(256) DEFAULT NULL COMMENT '<NOT USED>',
  `include_in_export` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1 = Include column in export, 0 = Exclude column in export',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`table_id`,`column_name`,`column_id`),
  UNIQUE KEY `column_id` (`column_id`),
  CONSTRAINT `FK_export_columns_export_tables` FOREIGN KEY (`table_id`) REFERENCES `export_tables` (`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=200453 DEFAULT CHARSET=latin1 COMMENT='This table contains all columns that exists on all tables that we are exporting. Unlike the export_tables table, this one gets created automatically by the export tool';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.export_tables
CREATE TABLE IF NOT EXISTS `export_tables` (
  `dbalias` varchar(256) NOT NULL COMMENT 'Database connection name that we export to',
  `target_schema` varchar(256) NOT NULL COMMENT 'Schema on the target system',
  `target_table` varchar(256) NOT NULL COMMENT 'Table on the target system',
  `table_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier of the table',
  `export_type` varchar(16) NOT NULL DEFAULT 'full' COMMENT 'What export method to use. Only full and incr is supported.',
  `hive_db` varchar(256) NOT NULL COMMENT 'Name of Hive Database that we export from',
  `hive_table` varchar(256) NOT NULL COMMENT 'Name of Hive Table that we export from',
  `last_update_from_hive` datetime DEFAULT NULL COMMENT 'Timestamp of last schema update from Hive',
  `sql_where_addition` varchar(1024) DEFAULT NULL COMMENT 'Will be added AFTER the SQL WHERE. If it''s an incr export, this will be after the incr limit statements. Example "orderId > 1000"',
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Will the table be included in Airflow DAG when it matches the DAG selection',
  `export_history` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'If set to 1, the DBImport definition is used for the hive_table without ''_history'' at the end of the tablename',
  `source_is_view` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Identifies if the Hive Table really is a view. Dont change this manually',
  `source_is_acid` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Identifies if the Hive Table are using acid. Dont change this manually',
  `validate_export` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1 = Validate the export once it''s done. 0 = Disable validation',
  `uppercase_columns` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto (Oracle = uppercase, other databases = lowercase)',
  `truncate_target` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1 = Truncate the target table before we export the data',
  `mappers` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto, 0 = invalid. Auto updated by ''export_main.sh''',
  `hive_rowcount` bigint(20) DEFAULT NULL COMMENT 'Number of rows in Hive table. Dont change manually',
  `target_rowcount` bigint(20) DEFAULT NULL COMMENT 'Number of rows in Target table. Dont change manually',
  `incr_column` varchar(256) DEFAULT NULL COMMENT 'The column in the Hive table that will be used to identify new rows for the incremental export. Must be a timestamp column',
  `incr_minvalue` varchar(25) DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually',
  `incr_maxvalue` varchar(25) DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually',
  `incr_minvalue_pending` varchar(25) DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually',
  `incr_maxvalue_pending` varchar(25) DEFAULT NULL COMMENT 'Used by incremental exports to keep track of progress. Dont change manually',
  `sqoop_options` varchar(1024) DEFAULT NULL COMMENT 'Sqoop options to use during export.',
  `sqoop_last_size` bigint(20) DEFAULT NULL COMMENT 'Last sqoop execution resulted in this size. Dont change manually',
  `sqoop_last_rows` bigint(20) DEFAULT NULL COMMENT 'Last sqoop execution resulted in many rows. Dont change manually',
  `create_target_table_sql` text COMMENT 'SQL statement that was used to create the target table. Dont change manually',
  `operator_notes` text COMMENT 'Free text field to write a note about the export. ',
  PRIMARY KEY (`dbalias`,`target_schema`,`target_table`),
  UNIQUE KEY `table_id` (`table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6981426 DEFAULT CHARSET=latin1 COMMENT='Main table where all tables that we can export are stored. ';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_columns
CREATE TABLE IF NOT EXISTS `import_columns` (
  `table_id` int(10) unsigned NOT NULL COMMENT 'Foreign Key to import_tables column ''table_id''',
  `column_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier of the column',
  `column_order` int(10) unsigned DEFAULT NULL COMMENT 'In what order does the column exist in the source system. ',
  `column_name` varchar(256) NOT NULL COMMENT 'Name of column in Hive. Dont change this manually',
  `hive_db` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `hive_table` varchar(256) DEFAULT NULL COMMENT 'Only used to make it easier to read the table. No real usage',
  `source_column_name` varchar(256) NOT NULL COMMENT 'Name of column in source system. Dont change this manually',
  `column_type` varchar(2048) NOT NULL COMMENT 'Column type in Hive. Dont change this manually',
  `source_column_type` varchar(2048) NOT NULL COMMENT 'Column type in source system. Dont change this manually',
  `source_database_type` varchar(256) DEFAULT NULL COMMENT 'That database type was the column imported from',
  `sqoop_column_type` varchar(256) DEFAULT NULL COMMENT 'Used to create a correct --map-column-java setting for sqoop. ',
  `force_string` tinyint(4) NOT NULL DEFAULT '-1' COMMENT 'If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in import_tables and jdbc_connections table',
  `include_in_import` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1 = Include column in import, 0 = Exclude column in import',
  `source_primary_key` tinyint(4) DEFAULT NULL COMMENT 'Number starting from 1 listing the order of the column in the PK. Dont change this manually',
  `last_update_from_source` datetime NOT NULL COMMENT 'Timestamp of last schema update from source',
  `comment` text COMMENT 'The column comment from the source system',
  `operator_notes` text COMMENT 'Free text field to write a note about the column',
  PRIMARY KEY (`table_id`,`column_id`,`column_name`),
  UNIQUE KEY `column_id` (`column_id`),
  KEY `table_id_source_column_name` (`table_id`,`source_column_name`),
  CONSTRAINT `FK__import_tables` FOREIGN KEY (`table_id`) REFERENCES `import_tables` (`table_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=867428 DEFAULT CHARSET=latin1 COMMENT='This table contains all columns that exists on all tables that we are importing. Unlike the import_tables table, this one gets created automatically by the ''Get Source TableSchema'' stage. ';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_failure_log
CREATE TABLE IF NOT EXISTS `import_failure_log` (
  `hive_db` varchar(256) NOT NULL COMMENT 'Hive Database',
  `hive_table` varchar(256) NOT NULL COMMENT 'Hive Table',
  `eventtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Time when error/warning occurred',
  `severity` text COMMENT 'The Severity of the event. ',
  `import_type` text COMMENT 'The import method used',
  `error_text` text COMMENT 'Text describing the failure'
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='If there is an error or a warning during import, bu the import still continues, these errors are logged in this table. An example could be that  a column cant be altered, foreign key not created, no new columns can be added and such.';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_foreign_keys
CREATE TABLE IF NOT EXISTS `import_foreign_keys` (
  `table_id` int(10) unsigned NOT NULL COMMENT 'Table ID in import_tables that have the FK',
  `column_id` int(10) unsigned NOT NULL COMMENT 'Column ID in import_columns that have the FK',
  `fk_index` int(10) unsigned NOT NULL COMMENT 'Index of FK',
  `fk_table_id` int(10) unsigned NOT NULL COMMENT 'Table ID in import_tables that the table is having a reference against',
  `fk_column_id` int(10) unsigned NOT NULL COMMENT 'Column ID in import_columns that the table is having a reference against',
  `key_position` int(10) unsigned NOT NULL COMMENT 'Position of the key',
  PRIMARY KEY (`table_id`,`column_id`,`fk_index`),
  KEY `FK_import_foreign_keys_import_columns_FK` (`fk_table_id`,`fk_column_id`),
  CONSTRAINT `FK_import_foreign_keys_import_columns` FOREIGN KEY (`table_id`, `column_id`) REFERENCES `import_columns` (`table_id`, `column_id`) ON DELETE CASCADE,
  CONSTRAINT `FK_import_foreign_keys_import_columns_FK` FOREIGN KEY (`fk_table_id`, `fk_column_id`) REFERENCES `import_columns` (`table_id`, `column_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='All foreign key definitions is saved in this table. The information in this table is recreated all the time, so no manually changes are allowed here. For a better understanding of this table, please use the view called import_foreign_keys_view instead';

-- Data exporting was unselected.
-- Dumping structure for view DBImport.import_foreign_keys_VIEW
-- Creating temporary table to overcome VIEW dependency errors
CREATE TABLE `import_foreign_keys_VIEW` (
	`hive_db` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`hive_table` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`fk_index` INT(10) UNSIGNED NOT NULL COMMENT 'Index of FK',
	`column_name` VARCHAR(256) NOT NULL COMMENT 'Name of column in Hive. Dont change this manually' COLLATE 'latin1_swedish_ci',
	`ref_hive_Db` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`ref_hive_table` VARCHAR(256) NULL COMMENT 'Only used to make it easier to read the table. No real usage' COLLATE 'latin1_swedish_ci',
	`ref_column_name` VARCHAR(256) NOT NULL COMMENT 'Name of column in Hive. Dont change this manually' COLLATE 'latin1_swedish_ci'
) ENGINE=MyISAM;

-- Dumping structure for tabell DBImport.import_retries_log
CREATE TABLE IF NOT EXISTS `import_retries_log` (
  `hive_db` varchar(256) NOT NULL COMMENT 'Hive DB',
  `hive_table` varchar(256) NOT NULL COMMENT 'Hive Table',
  `retry_time` datetime NOT NULL COMMENT 'Time when the retry was started',
  `stage` int(11) NOT NULL COMMENT 'The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG''s',
  `stage_description` text NOT NULL COMMENT 'Description of the stage',
  `import_type` text COMMENT '<NOT USED>',
  `unrecoverable_error` tinyint(4) DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_start` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_stop` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_duration` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_start` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_stop` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_duration` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_mappers` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_rows` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_size` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `source_table_rowcount` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `target_table_rowcount` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `incr_minvalue` text COMMENT '<NOT USED>',
  `incr_maxvalue` text COMMENT '<NOT USED>',
  `incr_column` text COMMENT '<NOT USED>',
  `logdir` text COMMENT '<NOT USED>',
  `timefile` text COMMENT '<NOT USED>',
  PRIMARY KEY (`hive_db`,`hive_table`,`retry_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Log of all retries that have happened. ';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_stage
CREATE TABLE IF NOT EXISTS `import_stage` (
  `hive_db` varchar(256) NOT NULL COMMENT 'Hive Database',
  `hive_table` varchar(256) NOT NULL COMMENT 'Hive Table',
  `stage` int(11) NOT NULL COMMENT 'Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG''s',
  `stage_description` text NOT NULL COMMENT 'Description of the stage',
  `import_type` text COMMENT '<NOT USED>',
  `unrecoverable_error` tinyint(4) DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_start` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_stop` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `get_source_rowcount_duration` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_start` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_stop` datetime DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_duration` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_mappers` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_rows` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `sqoop_size` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `source_table_rowcount` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `target_table_rowcount` bigint(20) DEFAULT NULL COMMENT '<NOT USED>',
  `incr_minvalue` text COMMENT '<NOT USED>',
  `incr_maxvalue` text COMMENT '<NOT USED>',
  `incr_column` text COMMENT '<NOT USED>',
  `logdir` text COMMENT '<NOT USED>',
  `timefile` text COMMENT '<NOT USED>',
  PRIMARY KEY (`hive_db`,`hive_table`,`stage`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.import_tables
CREATE TABLE IF NOT EXISTS `import_tables` (
  `hive_db` varchar(256) NOT NULL COMMENT 'Hive Database to import to',
  `hive_table` varchar(256) NOT NULL COMMENT 'Hive Table to import to',
  `table_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier',
  `dbalias` varchar(256) NOT NULL COMMENT 'Name of database connection from jdbc_connections table',
  `source_schema` varchar(256) NOT NULL COMMENT 'Name of the schema in the remote database',
  `source_table` varchar(256) NOT NULL COMMENT 'Name of the table in the remote database',
  `import_type` varchar(32) NOT NULL DEFAULT 'full' COMMENT 'What import method to use',
  `last_update_from_source` datetime DEFAULT NULL COMMENT 'Timestamp of last schema update from source',
  `sqoop_sql_where_addition` varchar(1024) DEFAULT NULL COMMENT 'Will be added AFTER the SQL WHERE. If it''s an incr import, this will be after the incr limit statements. Example "orderId > 1000"',
  `nomerge_ingestion_sql_addition` varchar(2048) DEFAULT NULL COMMENT 'This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables',
  `include_in_airflow` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Will the table be included in Airflow DAG when it matches the DAG selection',
  `airflow_priority` tinyint(4) DEFAULT NULL COMMENT 'This will set priority_weight in Airflow',
  `validate_import` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Should the import be validated',
  `validate_diff_allowed` bigint(20) NOT NULL DEFAULT '-1' COMMENT '-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have',
  `truncate_hive` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Truncate Hive table before loading it. ',
  `mappers` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 = auto or positiv number for a fixed number of mappers. If Auto, then it''s calculated based of last sqoop import size',
  `soft_delete_during_merge` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'If 1, then the row will be marked as deleted instead of actually being removed from the table. Only used for Merge imports',
  `source_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `hive_rowcount` bigint(20) DEFAULT NULL COMMENT 'Used for validation. Dont change manually',
  `incr_mode` varchar(16) DEFAULT NULL COMMENT 'append or lastmodified',
  `incr_column` varchar(256) DEFAULT NULL COMMENT 'What column to use to identify new rows',
  `incr_minvalue` varchar(32) DEFAULT NULL COMMENT 'Used for incremental imports. Dont change manually',
  `incr_maxvalue` varchar(32) DEFAULT NULL COMMENT 'Used for incremental imports. Dont change manually',
  `incr_minvalue_pending` varchar(32) DEFAULT NULL COMMENT 'Used for incremental imports. Dont change manually',
  `incr_maxvalue_pending` varchar(32) DEFAULT NULL COMMENT 'Used for incremental imports. Dont change manually',
  `pk_column_override` varchar(1024) DEFAULT NULL COMMENT 'Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns',
  `pk_column_override_mergeonly` varchar(1024) DEFAULT NULL COMMENT 'Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns',
  `hive_merge_heap` int(11) DEFAULT NULL COMMENT 'Should be a multiple of Yarn container size. If NULL then it will use the default specified in Yarn and TEZ',
  `concatenate_hive_table` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '<NOT USED>',
  `sqoop_query` text COMMENT 'Use a custom query in sqoop to read data from source table',
  `sqoop_options` text COMMENT 'Options to send to sqoop. Most common used for --split-by option',
  `sqoop_last_size` bigint(20) DEFAULT NULL COMMENT 'Used to track sqoop operation. Dont change manually',
  `sqoop_last_rows` bigint(20) DEFAULT NULL COMMENT 'Used to track sqoop operation. Dont change manually',
  `sqoop_last_execution` bigint(20) DEFAULT NULL COMMENT 'Used to track sqoop operation. Dont change manually',
  `sqoop_use_generated_sql` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '1 = Use the generated SQL that is saved in the generated_sqoop_query column',
  `sqoop_allow_text_splitter` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Allow splits on text columns. Use with caution',
  `force_string` tinyint(4) NOT NULL DEFAULT '-1' COMMENT 'If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in jdbc_connections table',
  `comment` text COMMENT 'Table comment from source system. Dont change manually',
  `generated_hive_column_definition` text COMMENT 'Generated column definition for Hive create table. Dont change manually',
  `generated_sqoop_query` text COMMENT 'Generated query for sqoop. Dont change manually',
  `generated_sqoop_options` text COMMENT 'Generated options for sqoop. Dont change manually',
  `generated_pk_columns` text COMMENT 'Generated Primary Keys. Dont change manually',
  `generated_foreign_keys` text COMMENT '<NOT USED>',
  `datalake_source` varchar(256) DEFAULT NULL COMMENT 'This value will come in the dbimport_source column if present. Overrides the same setting in jdbc_connections table',
  `operator_notes` text COMMENT 'Free text field to write a note about the import. ',
  PRIMARY KEY (`hive_db`,`hive_table`),
  UNIQUE KEY `table_id` (`table_id`),
  KEY `hive_db_dbalias_source_schema_source_table` (`hive_db`,`dbalias`,`source_schema`,`source_table`)
) ENGINE=InnoDB AUTO_INCREMENT=8000224 DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC COMMENT='Main table where all tables that we can import are stored. ';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.jdbc_connections
CREATE TABLE IF NOT EXISTS `jdbc_connections` (
  `dbalias` varchar(256) NOT NULL COMMENT 'Name of the Database connection',
  `private_key_path` varchar(128) DEFAULT NULL COMMENT '<NOT USED>',
  `public_key_path` varchar(128) DEFAULT NULL COMMENT '<NOT USED>',
  `jdbc_url` text NOT NULL COMMENT 'The JDBC URL String',
  `credentials` text COMMENT 'Encrypted fields for credentials.m Changed by the saveCredentialTool',
  `datalake_source` varchar(256) DEFAULT NULL COMMENT 'This value will come in the dbimport_source column if present. Priority is table, connection',
  `force_string` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'If set to 1, all character based fields (char, varchar) will become string in Hive',
  `create_datalake_import` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'If set to 1, the datalake_import column will be created on all tables that is using this dbalias',
  `timewindow_start` time DEFAULT NULL COMMENT 'Start of the time window when we are allowed to run against this connection.',
  `timewindow_stop` time DEFAULT NULL COMMENT 'End of the time window when we are allowed to run against this connection.',
  `operator_notes` text COMMENT 'Free text field to write a note about the connection',
  PRIMARY KEY (`dbalias`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Database connection definitions';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.json_to_rest
CREATE TABLE IF NOT EXISTS `json_to_rest` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'Unique Identifier',
  `endpoint` text NOT NULL COMMENT 'The REST interface to send the json data to ',
  `status` tinyint(4) NOT NULL COMMENT 'Internal status to keep track of what the status of the transmissions is',
  `jsondata` text NOT NULL COMMENT 'The payload to send',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11776 DEFAULT CHARSET=latin1 COMMENT='Temporary storage of JSON payloads that will be sent to a REST interface if the tool is configured to do so.';

-- Data exporting was unselected.
-- Dumping structure for tabell DBImport.table_change_history
CREATE TABLE IF NOT EXISTS `table_change_history` (
  `hive_db` varchar(256) NOT NULL,
  `hive_table` varchar(256) NOT NULL,
  `column_name` varchar(256) NOT NULL,
  `eventtime` datetime NOT NULL,
  `event` varchar(128) NOT NULL,
  `previous_value` varchar(32) DEFAULT NULL,
  `value` varchar(32) DEFAULT NULL,
  `description` varchar(256) NOT NULL,
  KEY `hive_db_hive_table` (`hive_db`,`hive_table`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='This table keeps track of all changes that was done to an abject after the initial load. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table';

-- Data exporting was unselected.
-- Dumping structure for view DBImport.airflow_dag_triggers
-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `airflow_dag_triggers`;
CREATE ALGORITHM=UNDEFINED DEFINER=`dbimport`@`%` SQL SECURITY DEFINER VIEW `airflow_dag_triggers` AS select `airflow_import_dag_execution`.`dag_name` AS `dag_name`,`airflow_import_dag_execution`.`task_name` AS `trigger_name`,`airflow_import_dag_execution`.`task_config` AS `dag_to_trigger` from `airflow_import_dag_execution`;

-- Dumping structure for view DBImport.import_foreign_keys_VIEW
-- Removing temporary table and create final VIEW structure
DROP TABLE IF EXISTS `import_foreign_keys_VIEW`;
CREATE ALGORITHM=UNDEFINED dbimport`@`%` SQL SECURITY DEFINER VIEW `import_foreign_keys_VIEW` AS select `s`.`hive_db` AS `hive_db`,`s`.`hive_table` AS `hive_table`,`fk`.`fk_index` AS `fk_index`,`s`.`column_name` AS `column_name`,`ref`.`hive_db` AS `ref_hive_Db`,`ref`.`hive_table` AS `ref_hive_table`,`ref`.`column_name` AS `ref_column_name` from ((`import_foreign_keys` `fk` join `import_columns` `s` on(((`s`.`table_id` = `fk`.`table_id`) and (`s`.`column_id` = `fk`.`column_id`)))) join `import_columns` `ref` on(((`ref`.`table_id` = `fk`.`fk_table_id`) and (`ref`.`column_id` = `fk`.`fk_column_id`)))) order by `fk`.`fk_index`,`fk`.`key_position`;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;

