export type FilterMapping = { [key: string]: string }

export const nameDisplayMappings: { [key: string]: FilterMapping } = {
  importPhaseType: {
    full: 'Full',
    incr: 'Incremental',
    oracle_flashback: 'Oracle Flashback',
    mssql_change_tracking: 'MSSQL Change Tracking'
  },
  etlPhaseType: {
    truncate_insert: 'Truncate and Insert',
    insert: 'Insert only',
    merge: 'Merge',
    merge_history_audit: 'Merge with History Audit',
    external_table: 'Only create external table',
    none: 'None'
  },
  importTool: {
    spark: 'Spark',
    sqoop: 'Sqoop'
  },
  etlEngine: {
    hive: 'Hive',
    spark: 'Spark'
  },
  validationMethod: {
    customQuery: 'Custom Query',
    rowCount: 'Row Count'
  },
  validateSource: {
    query: 'Query before import',
    sqoop: 'Imported rows'
  },
  incrMode: {
    append: 'Append',
    lastModified: 'Last modified'
  },
  incrValidationMethod: {
    full: 'Full',
    incr: 'Incremental'
  },
  mergeCompactionMethod: {
    default: 'Default',
    none: 'None',
    minor: 'Minor Compaction',
    minor_and_wait: 'Minor Compaction and Wait',
    major: 'Major Compaction',
    major_and_wait: 'Major Compaction and Wait'
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
