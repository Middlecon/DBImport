// Enum mapping, string to enum

export function mapEnumValue<T extends string>(
  value: string,
  validValues: T[],
  fallback: 'Unknown'
): T | 'Unknown' {
  return validValues.includes(value as T) ? (value as T) : fallback
}

// Setting type

export enum SettingType {
  Boolean = 'boolean',
  BooleanOrAuto = 'booleanOrAuto(-1)',
  BooleanOrDefaultFromConfig = 'booleanOrDefaultFromConfig(-1)',
  BooleanOrDefaultFromConnection = 'booleanOrDefaultFromConnection(-1)',
  Readonly = 'readonly',
  Text = 'text',
  Enum = 'enum',
  Reference = 'reference',
  Hidden = 'hidden', // Needed here or handled in other way?
  IntegerFromZeroOrNull = 'integerFromZeroOrNull',
  IntegerFromOneOrNull = 'integerFromOneOrNull',
  IntegerFromZeroOrAuto = 'integerFromZeroOrAuto(-1)',
  IntegerFromOneOrAuto = 'integerFromOneOrAuto(-1)',
  IntegerFromOneOrDefaultFromConfig = 'integerFromOneOrDefaultFromConfig(null)'
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
