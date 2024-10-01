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
import { UITable } from './interfaces'

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
    [ValidateSource.Sqoop]: 'Imported rows'
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

export const reverseNameMappings: { [key: string]: FilterMapping } = {
  importPhaseType: {
    Full: ImportType.Full,
    Incremental: ImportType.Incremental,
    'Oracle Flashback': ImportType.OracleFlashback,
    'MSSQL Change Tracking': ImportType.MSSQLChangeTracking
  },
  etlPhaseType: {
    'Truncate and Insert': EtlType.TruncateAndInsert,
    'Insert only': EtlType.InsertOnly,
    Merge: EtlType.Merge,
    'Merge with History Audit': EtlType.MergeHistoryAudit,
    'Only create external table': EtlType.External,
    None: EtlType.None
  },
  importTool: {
    Spark: ImportTool.Spark,
    Sqoop: ImportTool.Sqoop
  },
  etlEngine: {
    Hive: EtlEngine.Hive,
    Spark: EtlEngine.Spark
  },
  validationMethod: {
    'Custom Query': ValidationMethod.CustomQuery,
    'Row Count': ValidationMethod.RowCount
  },
  validateSource: {
    'Query before import': ValidateSource.Query,
    'Imported rows': ValidateSource.Sqoop
  },
  incrMode: {
    Append: IncrMode.Append,
    'Last modified': IncrMode.LastModified
  },
  incrValidationMethod: {
    Full: IncrValidationMethod.Full,
    Incremental: IncrValidationMethod.Incremental
  },
  mergeCompactionMethod: {
    Default: MergeCompactionMethod.Default,
    None: MergeCompactionMethod.None,
    'Minor Compaction': MergeCompactionMethod.Minor,
    'Minor Compaction and Wait': MergeCompactionMethod.MinorAndWait,
    'Major Compaction': MergeCompactionMethod.Major,
    'Major Compaction and Wait': MergeCompactionMethod.MajorAndWait
  }
}

export const labelToUITableKey: { [label: string]: keyof UITable } = {
  Database: 'database',
  Table: 'table',
  Connection: 'connection',
  'Source Schema': 'sourceSchema',
  'Source Table': 'sourceTable',
  'Import Type': 'importPhaseType',
  'ETL Type': 'etlPhaseType',
  'Import Tool': 'importTool',
  'ETL Engine': 'etlEngine',
  'Last Update From Source': 'lastUpdateFromSource',
  'Source Table Type': 'sourceTableType',
  'Import Database': 'importDatabase',
  'Import Table': 'importTable',
  'History Database': 'historyDatabase',
  'History Table': 'historyTable',
  'Truncate Table': 'truncateTable',
  'Allow Text Splitter': 'allowTextSplitter',
  'Force String': 'forceString',
  'Split By Column': 'splitByColumn',
  'Sqoop Options': 'sqoopOptions',
  'SQL WHERE Addition': 'sqlWhereAddition',
  'Custom Query': 'customQuery',
  'Custom Max Query': 'customMaxQuery',
  'Use Generated SQL': 'useGeneratedSql',
  'No Merge Ingestion SQL Addition': 'nomergeIngestionSqlAddition',
  'Last Size': 'lastSize',
  'Last Rows': 'lastRows',
  'Last Mappers': 'lastMappers',
  'Generated Hive Column Definition': 'generatedHiveColumnDefinition',
  'Generated Sqoop Query': 'generatedSqoopQuery',
  'Generated Sqoop Options': 'generatedSqoopOptions',
  'Generated Primary Key Columns': 'generatedPkColumns',
  'Generated Foreign Keys': 'generatedForeignKeys',
  'Incremental Min Value': 'incrMinvalue',
  'Incremental Max Value': 'incrMaxvalue',
  'Pending Min Value': 'incrMinvaluePending',
  'Pending Max Value': 'incrMaxvaluePending',
  'Incremental Mode': 'incrMode',
  'Incremental Column': 'incrColumn',
  'Incremental Validation Method': 'incrValidationMethod',
  'Create Foreign Keys': 'createForeignKeys',
  'Primary Key Override': 'pkColumnOverride',
  'Primary Key Override (Merge only)': 'pkColumnOverrideMergeonly',
  'Invalidate Impala': 'invalidateImpala',
  'Soft Delete During Merge': 'softDeleteDuringMerge',
  'Merge Compaction Method': 'mergeCompactionMethod',
  'Datalake Source': 'datalakeSource',
  Mappers: 'mappers',
  'Hive Split Count': 'splitCount',
  'Hive Java Heap (MB)': 'mergeHeap',
  'Spark Executor Memory': 'sparkExecutorMemory',
  'Spark Executors': 'sparkExecutors',
  'Validate Import': 'validateImport',
  'Validation Method': 'validationMethod',
  'Validate Source': 'validateSource',
  'Allowed Validation Difference': 'validateDiffAllowed',
  'Custom Query Source SQL': 'validationCustomQuerySourceSQL',
  'Custom Query Hive SQL': 'validationCustomQueryHiveSQL',
  'Source Row Count': 'sourceRowcount',
  'Source Row Count Incremental': 'sourceRowcountIncr',
  'Target Row Count': 'targetRowcount',
  'Validate Import Table': 'validationCustomQueryValidateImportTable',
  'Custom Query Source Value': 'validationCustomQuerySourceValue',
  'Custom Query Hive Value': 'validationCustomQueryHiveValue',
  'Airflow Priority': 'airflowPriority',
  'Include in Airflow': 'includeInAirflow',
  'Operator Notes': 'operatorNotes',
  'Copy Finished': 'copyFinished',
  'Copy Slave': 'copySlave'
}

export function getKeyFromLabel(label: string): keyof UITable | undefined {
  return labelToUITableKey[label]
}

export const reverseMapDisplayValue = (
  filterKey: string,
  filterValue: string
): string => {
  const mapping = reverseNameMappings[filterKey]
  return mapping?.[filterValue] || filterValue
}

export const nameReverseMappings: {
  [category: string]: { [key: string]: string }
} = {
  'Import Type': {
    Full: ImportType.Full,
    Incremental: ImportType.Incremental,
    'Oracle Flashback': ImportType.OracleFlashback,
    'MSSQL Change Tracking': ImportType.MSSQLChangeTracking
  },
  'ETL Type': {
    'Truncate and Insert': EtlType.TruncateAndInsert,
    'Insert Only': EtlType.InsertOnly,
    Merge: EtlType.Merge,
    'Merge with History Audit': EtlType.MergeHistoryAudit,
    'Only create external table': EtlType.External,
    None: EtlType.None
  },
  'Import Tool': {
    Spark: ImportTool.Spark,
    Sqoop: ImportTool.Sqoop
  },
  'ETL Engine': {
    Hive: EtlEngine.Hive,
    Spark: EtlEngine.Spark
  },
  'Validation Method': {
    'Custom Query': ValidationMethod.CustomQuery,
    'Row Count': ValidationMethod.RowCount
  },
  'Validate Source': {
    'Query before import': ValidateSource.Query,
    'Imported rows': ValidateSource.Sqoop
  },
  'Incremental Mode': {
    Append: IncrMode.Append,
    'Last Modified': IncrMode.LastModified
  },
  'Incremental Validation Method': {
    Full: IncrValidationMethod.Full,
    Incremental: IncrValidationMethod.Incremental
  },
  'Merge Compaction Method': {
    Default: MergeCompactionMethod.Default,
    None: MergeCompactionMethod.None,
    'Minor Compaction': MergeCompactionMethod.Minor,
    'Minor Compaction and Wait': MergeCompactionMethod.MinorAndWait,
    'Major Compaction': MergeCompactionMethod.Major,
    'Major Compaction and Wait': MergeCompactionMethod.MajorAndWait
  }
}

export const reverseMapEnumValue = (
  category: string,
  displayValue: string
): string => {
  const categoryMappings = nameReverseMappings[category]
  if (!categoryMappings) {
    console.warn(`No mappings found for category: ${category}`)
    return 'unknown'
  }

  const backendValue = categoryMappings[displayValue]
  return backendValue || 'unknown' // Maybe have another fallback
}
