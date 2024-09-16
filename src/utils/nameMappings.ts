export type FilterMapping = { [key: string]: string }

const filterMappings: { [key: string]: FilterMapping } = {
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
  }
}

export const mapDisplayValue = (key: string, value: string): string => {
  return filterMappings[key]?.[value] || value
}

export const nameMappings: { [key: string]: FilterMapping } = {
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

export const mapFilterValue = (
  filterKey: string,
  filterValue: string
): string => {
  const mapping = nameMappings[filterKey]
  return mapping?.[filterValue] || filterValue
}

export const mapSelectedFilters = (selectedFilters: {
  [key: string]: string[]
}): { [key: string]: string[] } => {
  const mappedFilters: { [key: string]: string[] } = {}

  for (const key in selectedFilters) {
    const mappedValues = selectedFilters[key].map((value) =>
      mapFilterValue(key, value)
    )
    mappedFilters[key] = mappedValues
  }

  return mappedFilters
}

export const reverseMapFilterValue = (title: string, value: string) => {
  const mapping: { [key: string]: { [key: string]: string } } = {
    'Import Type': {
      full: 'Full',
      incr: 'Incremental',
      oracle_flashback: 'Oracle Flashback',
      mssql_change_tracking: 'MSSQL Change Tracking'
    },
    'ETL Type': {
      truncate_insert: 'Truncate and Insert',
      insert: 'Insert only',
      merge: 'Merge',
      merge_history_audit: 'Merge with History Audit',
      external_table: 'Only create external table',
      none: 'None'
    },
    'Import Tool': {
      spark: 'Spark',
      sqoop: 'Sqoop'
    },
    'ETL Engine': {
      hive: 'Hive',
      spark: 'Spark'
    }
  }

  return mapping[title]?.[value] || value
}
