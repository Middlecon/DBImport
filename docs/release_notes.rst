Release Notes
=============

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
