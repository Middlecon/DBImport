import {
  EtlEngine,
  EtlType,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  ValidateSource,
  ValidationMethod
} from './enums'

export type FilterMapping = { [key: string]: string }

export const nameDisplayMappings: { [key: string]: FilterMapping } = {
  importPhaseType: {
    [ImportType.Full]: 'Full',
    [ImportType.Incremental]: 'Incremental',
    [ImportType.OracleFlashback]: 'Oracle Flashback',
    [ImportType.MSSQLChangeTracking]: 'MSSQL Change Tracking'
  },
  etlPhaseType: {
    [EtlType.TruncateAndInsert]: 'Truncate and Insert',
    [EtlType.InsertOnly]: 'Insert only',
    [EtlType.Merge]: 'Merge',
    [EtlType.MergeHistoryAudit]: 'Merge with History Audit',
    [EtlType.External]: 'Only create external table',
    [EtlType.None]: 'None'
  },
  importTool: {
    [ImportTool.Spark]: 'Spark',
    [ImportTool.Sqoop]: 'Sqoop'
  },
  etlEngine: {
    [EtlEngine.Hive]: 'Hive',
    [EtlEngine.Spark]: 'Spark'
  },
  validationMethod: {
    [ValidationMethod.CustomQuery]: 'Custom Query',
    [ValidationMethod.RowCount]: 'Row Count'
  },
  validateSource: {
    [ValidateSource.Query]: 'Query before import',
    [ValidateSource.Scoop]: 'Imported rows'
  },
  incrMode: {
    [IncrMode.Append]: 'Append',
    [IncrMode.LastModified]: 'Last modified'
  },
  incrValidationMethod: {
    [IncrValidationMethod.Full]: 'Full',
    [IncrValidationMethod.Incremental]: 'Incremental'
  },
  mergeCompactionMethod: {
    [MergeCompactionMethod.Default]: 'Default',
    [MergeCompactionMethod.None]: 'None',
    [MergeCompactionMethod.Minor]: 'Minor Compaction',
    [MergeCompactionMethod.MinorAndWait]: 'Minor Compaction and Wait',
    [MergeCompactionMethod.Major]: 'Major Compaction',
    [MergeCompactionMethod.MajorAndWait]: 'Major Compaction and Wait'
  }
}

export const mapDisplayValue = (key: string, value: string): string => {
  return nameDisplayMappings[key]?.[value] || value
}

const nameReverseMappings: { [key: string]: FilterMapping } = {
  'Import Type': {
    Full: 'full',
    Incremental: 'incr',
    'Oracle Flashback': 'oracle_flashback',
    'MSSQL Change Tracking': 'mssql_change_tracking'
  },
  'ETL Type': {
    'Truncate and Insert': 'truncate_insert',
    'Insert only': 'insert',
    Merge: 'merge',
    'Merge with History Audit': 'merge_history_audit',
    'Only create external table': 'external_table',
    None: 'none'
  },
  'Import Tool': {
    Spark: 'spark',
    Sqoop: 'sqoop'
  },
  'ETL Engine': {
    Hive: 'hive',
    Spark: 'spark'
  }
}

export const reverseMapDisplayValue = (
  filterKey: string,
  filterValue: string
): string => {
  const mapping = nameReverseMappings[filterKey]
  return mapping?.[filterValue] || filterValue
}

export const mapSelectedFilters = (selectedFilters: {
  [key: string]: string[]
}): { [key: string]: string[] } => {
  const mappedFilters: { [key: string]: string[] } = {}

  for (const key in selectedFilters) {
    const mappedValues = selectedFilters[key].map((value) =>
      reverseMapDisplayValue(key, value)
    )
    mappedFilters[key] = mappedValues
  }

  return mappedFilters
}
