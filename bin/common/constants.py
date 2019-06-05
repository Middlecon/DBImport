# DBImport specific
VERSION = "0.42"

# Database types
MYSQL = "mysql"
ORACLE = "oracle"
MSSQL = "sql_server"
POSTGRESQL = "postgresql"
PROGRESS = "progress_db"
DB2_UDB = "db2 udb"
DB2_AS400 = "db2 as400"
MONGO = "mongodb"

# Key constraints
PRIMARY_KEY = "P"
FOREIGN_KEY = "F"

# Phase defintions
IMPORT_PHASE_ORACLE_FLASHBACK = "oracle_flashback"
IMPORT_PHASE_FULL = "full"
IMPORT_PHASE_INCR = "incr"
COPY_PHASE_NONE = "none"
ETL_PHASE_NONE = "none"
ETL_PHASE_INSERT = "insert"
ETL_PHASE_TRUNCATEINSERT = "truncate_insert"
ETL_PHASE_MERGEHISTORYAUDIT = "merge_history_audit"
ETL_PHASE_MERGEONLY = "merge"
EXPORT_PHASE_FULL = "full"
EXPORT_PHASE_INCR = "incr"
