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
  Readonly = 'readonly',
  Text = 'text',
  Enum = 'enum',
  Integer = 'integer',
  Hidden = 'hidden', // Needed here or handled in other way?
  BooleanOrAuto = 'booleanOrAuto(-1)',
  IntegerOrAuto = 'integerOrAuto(-1)',
  BooleanOrDefaultFromConfig = 'booleanOrDefaultFromConfig(-1)'
}

// Table

export enum ImportType {
  Full = 'full',
  Incremental = 'incremental',
  OracleFlashback = 'oracle_flashback',
  MSSQLChangeTracking = 'mssql_change_tracking'
}

export enum EtlType {
  TruncateAndInsert = 'full',
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
  Scoop = 'sqoop'
}

export enum IncrMode {
  Append = 'append',
  LastModified = 'lastmodified'
}

export enum IncrValidationMethod {
  Full = 'full',
  Incremental = 'incremental'
}

export enum MergeCompactionMethod {
  Default = 'default',
  None = 'none',
  Minor = 'minor',
  MinorAndWait = 'minor_and_wait',
  Major = 'major',
  MajorAndWait = 'major_and_wait'
}
