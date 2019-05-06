Release Notes
=============

v0.30
-----

**Fixed issues**

  - Issue #10: MySQL decimal columns gets created without precision

**New Features**

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
