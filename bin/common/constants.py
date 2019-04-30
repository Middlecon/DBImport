# DBImport specific
VERSION = "0.21"

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
PHASE1_FULL = "Full"
PHASE1_INCR = "Incremental"
PHASE2_COPY = "Copy"
PHASE2_HISTORYAUDIT = "History Audit"
