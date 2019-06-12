Release Notes
=============

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
