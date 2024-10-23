// Setting type

export enum SettingType {
  Boolean = 'boolean',
  BooleanNumber = 'booleanNumber',
  BooleanOrDefaultFromConfig = 'booleanOrDefaultFromConfig(-1)',
  BooleanOrDefaultFromConnection = 'booleanOrDefaultFromConnection(-1)',
  Readonly = 'readonly',
  Text = 'text',
  Textarea = 'textarea',
  Enum = 'enum',
  ConnectionReference = 'connectionReference',
  ConnectionReferenceRequired = 'connectionReferenceRequired',
  Hidden = 'hidden', // Needed here or handled in other way?
  IntegerFromZeroOrNull = 'integerFromZeroOrNull',
  IntegerFromOneOrNull = 'integerFromOneOrNull',
  IntegerFromZeroOrAuto = 'integerFromZeroOrAuto(-1)',
  IntegerFromOneOrAuto = 'integerFromOneOrAuto(-1)',
  IntegerFromOneOrDefaultFromConfig = 'integerFromOneOrDefaultFromConfig(null)',
  Time = 'time',
  TimeZone = 'timezone',
  GroupingSpace = 'groupingSpace'
}

// Table

export enum ImportType {
  Full = 'full',
  Incremental = 'incr',
  OracleFlashback = 'oracle_flashback',
  MSSQLChangeTracking = 'mssql_change_tracking'
}

export enum EtlType {
  TruncateAndInsert = 'truncate_insert',
  InsertOnly = 'insert',
  Merge = 'merge',
  MergeHistoryAudit = 'merge_history_audit',
  External = 'external',
  None = 'none'
}

export enum ImportTool {
  Spark = 'spark',
  Sqoop = 'sqoop'
}

export enum EtlEngine {
  Hive = 'hive',
  Spark = 'spark'
}

export enum ValidationMethod {
  CustomQuery = 'customQuery',
  RowCount = 'rowCount'
}

export enum ValidateSource {
  Query = 'query',
  Sqoop = 'sqoop'
}

export enum IncrMode {
  Append = 'append',
  LastModified = 'lastmodified'
}

export enum IncrValidationMethod {
  Full = 'full',
  Incremental = 'incr'
}

export enum MergeCompactionMethod {
  Default = 'default',
  None = 'none',
  Minor = 'minor',
  MinorAndWait = 'minor_and_wait',
  Major = 'major',
  MajorAndWait = 'major_and_wait'
}

export enum AnonymizationFunction {
  None = 'None',
  Hash = 'Hash',
  ReplaceWithStar = 'Replace with star',
  ShowFirst4Chars = 'Show first 4 chars'
}

export enum AirflowDAGTaskType {
  ShellScript = 'shell script',
  HiveSQL = 'Hive SQL',
  HiveSQLScript = 'Hive SQL Script',
  JDBCSQL = 'JDBC SQL',
  TriggerDAG = 'Trigger DAG',
  DAGSensor = 'DAG Sensor',
  SQLSensor = 'SQL Sensor',
  DBImportCommand = 'DBImport command'
}

export enum AirflowDAGTaskPlacement {
  BeforeMain = 'before main',
  AfterMain = 'after main',
  InMain = 'in main'
}
