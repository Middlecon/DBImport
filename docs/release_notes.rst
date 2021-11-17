Release Notes
=============

v0.68 
------------------------------

**Fixed issues**

  - Issue #85: Incremental import of empty database might result in duplicate rows
  - Issue #86: Time start/stop on jdbc_connections does not follow scheduled timezone
  - Issue #87: Time start/stop on jdbc_connections does not follow scheduled timezone
  - Issue #88: Column comments from Oracle is missing

**New Features**

  - Source table information likes indexes are fetched from sources database and stored for information use
  - Deploy tool to handle deployments of DAGs and affected tables/columns/connections to a remote DBImport instance
  - Information of started and finised imports, exports and Airflow DAGs can be sent in json format to Kafka and REST interfaces


v0.67 
------------------------------

**New Features**

  - Support for imports with MSSQL Change Tracking functions


v0.66
------------------------------

**Fixed issues**

  - Issue #43: incr_mode in import_tables is not an Enum column
  - Issue #74: Tasks from custom_dags is not using retries setting
  - Issue #75: Add alias argument to manage script for encryptCredentials
  - Issue #76: SQL 'where' in export_tables is not working
  - Issue #77: Default value of etl_phase_type in import_tables is missing
  - Issue #78: ResetIncrementalExport dont truncate table
  - Issue #79: Airflow pool will get duplicate error if upper/lower case hostname differ

**New Features**

  - Custom SQL validation
  - Support for timezone
  - Atlas integration performance from DBImport server has been improved and can now scale with multiple threads


v0.65
------------------------------

**Fixed issues**

  - Issue #61: Import from View on MSSQL failes with no columns
  - Issue #62: DBImport Server connection retries against config database 
  - Issue #63: Control executors for Spark export
  - Issue #64: Initial incremental export with 0 rows fails
  - Issue #65: Python error when missing permissions in Atlas
  - Issue #66: Setting a timeslot where start is larger than stop gives a configuration error
  - Issue #67: Unneeded column change during export to Oracle
  - Issue #68: dropExportTable generates error if table does not exist
  - Issue #69: schedule_interval columns with different size
  - Issue #70: Support for authentication source for Mongo connections
  - Issue #71: testConnection against Mongo does not work
  - Issue #72: Incremental import of empty tables failes because no rows stored from source

**Changed behavior**

  - Hive connection supports multiple servers.
  - Poke interval for Multi-cluster sensors is changed from 30 sec to 5 min
  - You force DBImport to process all steps for incremental imports even if it contains no new data. 
  - Encrypt password for jdbc connections supports '-a' as an argument for connection name

**New Features**

  - Atlas Integration
  - Import from MongoDB is supported with Spark as import tool 
  - Export to PostgreSQL
  - Oracle CDC Import supports History table
  - Can force a major compaction after an Import that uses Hive Merge
  - Support for anonymization of columns during import

v0.64
------------------------------

**Fixed issues**

  - Issue #59: Export update_table failes because table is not empty

**New Features**

  - Spark supported for both import and exports (TechPreview)

v0.63
------------------------------

**Fixed issues**

  - Issue #60: Export tries to alter column types FLOAT(*)

**Improvments**

  - Better description of parameters in *manage* command

**Changed behavior**

  - Kerberos ticket is created and handled by DBImport internally. No need to have a valid ticket before start anymore

**New Features**

  - Dedicated *copy* command
  - Sqoop column type can be overridden with setting in import_columns

v0.62
------------------------------

**New Features**

  - Multi-cluster imports with asynchronous copy mode
  - DBImport server daemon. This is the service that handles asynchronous copy of data between clusters

v0.61
------------------------------

**Fixed issues**

  - Issue #39: Export failes when sqoop timeout against Kafka for Atlas info
  - Issue #40: Creating Airflow Pools failes when pool table is empty in Airflow
  - Issue #41: Error when creating DBImport database
  - Issue #42: Airflow Tasks failes 'In Main' if there is a dependency to a DBImport Task
  - Issue #44: Importing a table with a column called 'const' is not supported
  - Issue #45: Retries sometimes failes due to Hive connection
  - Issue #46: Exporting from a Hive table that doesnt exists gives errors
  - Issue #47: Get rowcount failes if column for incremental load is a reserved word
  - Issue #48: Column names containing # fails on column not found
  - Issue #49: Importing ‘time’ columns from MSSQL fails
  - Issue #50: SQL Server connection with encryption uses wrong JDBC driver
  - Issue #51: sqoop_sql_where_statement with validation = query failes with double where statements
  - Issue #52: column type 'long' in oracle gets wrong column type in Hive
  - Issue #53: No logging of forced removal of locks 
  - Issue #54: DB2 clob columns is not mapped to String in sqoop
  - Issue #55: DB2 import with column type time(3) result in null values
  - Issue #56: timestamp columns from MSSQL will result in NULL values
  - Issue #58: merge operations only look at mergeonly override for PK

**Improvments**

  - Foreign Keys can be disabled per table or connection 

v0.60
------------------------------

**Fixed issues**

  - Issue #30: manage generates error when no valid Kerberos ticket available
  - Issue #31: Oracle Flashback imports get Merge cardinality_violation
  - Issue #32: Airflow sensor never times out
  - Issue #33: truncate_hive column in import_tables is not used/implemented
  - Issue #34: pk_column_override and pk_column_override_mergeonly with uppercase columns failes
  - Issue #35: datalake_source is only created with a new table, not added to a already existing
  - Issue #36: sqoop mappers not based on history
  - Issue #37: changing HDFS_Basedir doesnt trigger an alter of the Import table
  - Issue #38: Wrong row count on exported tables

**Improvments**

  - HDFS basedir is configurable in the configuration table

**Changed behavior**

  - Configuration for HDFS are move to the configuration table in MySQL
  - Configuration for Sqoop mappers are move to the configuration table in MySQL

**New Features**

  - Multi-cluster imports (synchronous only)
  - *full_insert* import method

v0.51
------------------------------

**Fixed issues**

  - Issue #29: Duplicate column in statistics when changing import type without reset

**Improvments**

  - Possible to specify Java Heap for Export operations

**Changed behavior**

  - *hive_merge_heap* column in *import_tables* sets Java Heap for the entire Hive session, not just for Merge operations.

**New Features**

  - Airflow integration 

v0.50
------------------------------

**Fixed issues**

  - Issue #26: Schema changes in configuration database is not handled
  - Issue #27: String export to MSSQL into varchar gets converted everytime
  - Issue #28: Update column description on exported MSSQL table failes

**Improvments**

  - resetIncrementalImport is added to 'manage' in order to clear an incremental import and force the next import to start with a initial import 

**Changed behavior**

  - Configuration for Hive validation test and extended messages are move to the configuration table in MySQL

**New Features**

  - New import type called 'oracle_flashback_merge' is availble. Will use the *Oracle Flashback Version Query* to import changed rows into Hive

v0.42
------------------------------

**Fixed issues**

  - Issue #20: Going from Merge to non-merge imports fails because missing datalake_import column
  - Issue #22: Column starting with _ failed if it's part of Primary Key and merge operation is running
  - Issue #23: varchar(-1) from MSSQL generates error in Sqoop
  - Issue #24: Remove locks by force only in target table
  - Issue #25: column with the name 'int' is not supported

**Improvments**

  - Removing locks by force is configurable in the configuration table

**Changed behavior**

  - Configuration to Hive metastore must be changed to a SQLAlchemy connection string stored in the setting *hive_metastore_alchemy_conn* 

**New Features**

  - Hive Metastore SQL connection now uses SQLAlchemy. This enables more than MySQL as database type for Hive Metastore


v0.41.1
------------------------------

**Fixed issues**

  - Issue #17: Oracle Primary Key got columns from Unique key
  - Issue #18: Error if Merge run on table with only PK columns
  - Issue #19: Hive Merge implicit cast wont work with X number of columns
  - Issue #21: _ at the start of the column name generates errors during import

**Improvments**

  - Propper error message when table contains no primary key and a merge operation is running

v0.41
-----

**Fixed issues**

  - Issue #16: include_in_import for map-column-java is not affected

**Improvments**

  - Issue #15: Move JDBC Driver config to database

**New Features**

  - Functions to add import tables by searching for tables in source that we dont already have
  - Functions to add export tables by searching for tables in hive that we dont already have

v0.40
-----

**Fixed issues**

  - Issue #14: force_string settings in import_columns was not used

**New Features**

  - Exports to MsSQL, Oracle, MySQL and DB2 is fully supported


v0.30
-----

**Fixed issues**

  - Issue #13: sqoop_query not respected
  - Issue #12: Include_in_import not respected
  - Issue #11: Oracle Number(>10) column having java_column_type = Integer
  - Issue #10: MySQL decimal columns gets created without precision

**New Features**

  - Ability to override the name and type of the column in Hive
  - It's now possible to select where to get the number of rows from for the validation. sqoop or query
  - Support for Merge operation during ETL Phase, including History Audit tables
  - Import supports command options -I, -C and -E for running only Import, Copy or ETL Phase

**Changed behavior**

  - *Stage 1* is renamed to *Import Phase*. -1 command option still works against *import* for compability
  - *Stage 2* is renamed to *ETL Phase*. -2 command option still works against *import* for compability
  - The values in the column *sqoop_options* in *import_tables* will be converted to lowercase before added to sqoop

v0.21
-----

**Fixed issues**

  - Issue #9: PK with spaces in column name failes on --split-by
  - Issue #8: Columnnames with two spaces after each other failes in sqoop
  - Issue #6: MySQL cant handle " around column names

**New Features**

  - You can limit the number of sqoop mappers globaly on a database connection by specifying a positiv value in the column *max_import_sessions*
  - Import statistics is stored in table *import_statistics* and *import_statistics_last*

v0.20
-----

**Fixed issues**

  - Issue #5: Message about 'split-by-text' even if the column is an integer
  - Issue #4: Parquet cant handle SPACE in column name
  - Issue #3: TimeCheck failes before 10.00
  - Issue #2: 'sqoop_sql_where_addition' assumes 'where' is in config
  - Issue #1: Errors when running without an valid Kerberos ticket

**New Features**

  - Incremental Imports are now supported
  - Encryption of username/password with manage --encryptCredentials
  - Repair of incremental import with manage --repairIncrementalImport
  - Repair of all failed incremental imports with manage --repairAllIncrementalImports
  - It's possible to ignore the timeWindow by adding --ignoreTime to the import command
  - You can force an import to start from the begining by adding --resetStage to the import command
