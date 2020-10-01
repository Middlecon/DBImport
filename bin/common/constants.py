# DBImport specific
VERSION = "0.66 pre-release"

# Database types
MYSQL = "mysql"
ORACLE = "oracle"
MSSQL = "sql_server"
POSTGRESQL = "postgresql"
PROGRESS = "progress_db"
DB2_UDB = "db2 udb"
DB2_AS400 = "db2 as400"
MONGO = "mongo"
CACHEDB = "cache"

# Key constraints
PRIMARY_KEY = "P"
FOREIGN_KEY = "F"

# Phase defintions
IMPORT_PHASE_MONGO_FULL = "mongo_full"
IMPORT_PHASE_ORACLE_FLASHBACK = "oracle_flashback"
IMPORT_PHASE_FULL = "full"
IMPORT_PHASE_INCR = "incr"
IMPORT_PHASE_SLAVE = "slave"
COPY_PHASE_NONE = "none"
ETL_PHASE_NONE = "none"
ETL_PHASE_INSERT = "insert"
ETL_PHASE_TRUNCATEINSERT = "truncate_insert"
ETL_PHASE_MERGEHISTORYAUDIT = "merge_history_audit"
ETL_PHASE_MERGEONLY = "merge"
EXPORT_PHASE_FULL = "full"
EXPORT_PHASE_INCR = "incr"

VALIDATION_METHOD_ROWCOUNT = "rowCount"
VALIDATION_METHOD_CUSTOMQUERY = "customQuery"
