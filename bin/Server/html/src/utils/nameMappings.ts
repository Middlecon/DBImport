import {
  AirflowDAGTaskPlacement,
  AirflowDAGTaskType,
  AnonymizationFunction,
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
import {
  Columns,
  Connection,
  CustomAirflowDAG,
  ExportAirflowDAG,
  ImportAirflowDAG,
  TableCreateMapped,
  UITableWithoutEnum,
  WithDynamicKeys
} from './interfaces'

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
  },
  anonymizationFunction: {
    [AnonymizationFunction.None]: 'None',
    [AnonymizationFunction.Hash]: 'Hash',
    [AnonymizationFunction.ReplaceWithStar]: 'Replace with star',
    [AnonymizationFunction.ShowFirst4Chars]: 'Show first 4 chars'
  },
  // Airflow DAG task type:
  type: {
    [AirflowDAGTaskType.ShellScript]: 'Shell Script',
    [AirflowDAGTaskType.HiveSQL]: 'Hive SQL',
    [AirflowDAGTaskType.HiveSQLScript]: 'Hive SQL Script',
    [AirflowDAGTaskType.JDBCSQL]: 'JDBC SQL',
    [AirflowDAGTaskType.TriggerDAG]: 'Trigger DAG',
    [AirflowDAGTaskType.DAGSensor]: 'DAG Sensor',
    [AirflowDAGTaskType.SQLSensor]: 'SQL Sensor',
    [AirflowDAGTaskType.DBImportCommand]: 'DBImport command'
  },
  // Airflow DAG task placement:
  placement: {
    [AirflowDAGTaskPlacement.BeforeMain]: 'Before Main',
    [AirflowDAGTaskPlacement.AfterMain]: 'After Main',
    [AirflowDAGTaskPlacement.InMain]: 'In Main'
  }
}

export const getEnumOptions = (key: string) => nameDisplayMappings[key] || {}

export const labelToKeyMap: Record<
  string,
  keyof Omit<TableCreateMapped, 'columns'>
> = {
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
  'SQL Where Addition': 'sqlWhereAddition',
  'No Merge Ingestion SQL Addition': 'nomergeIngestionSqlAddition',
  'Include in Airflow': 'includeInAirflow',
  'Airflow Priority': 'airflowPriority',
  'Validate Import': 'validateImport',
  'Validation Method': 'validationMethod',
  'Validate Source': 'validateSource',
  'Validate Diff Allowed': 'validateDiffAllowed',
  'Validation Custom Query Source SQL': 'validationCustomQuerySourceSQL',
  'Validation Custom Query Hive SQL': 'validationCustomQueryHiveSQL',
  'Validation Custom Query Validate Import Table':
    'validationCustomQueryValidateImportTable',
  'Truncate Table': 'truncateTable',
  Mappers: 'mappers',
  'Soft Delete During Merge': 'softDeleteDuringMerge',
  'Incremental Mode': 'incrMode',
  'Incremental Column': 'incrColumn',
  'Incremental Validation Method': 'incrValidationMethod',
  'Primary Key Column Override': 'pkColumnOverride',
  'Primary Key Column Override Merge Only': 'pkColumnOverrideMergeonly',
  'Merge Heap': 'mergeHeap',
  'Split Count': 'splitCount',
  'Spark Executor Memory': 'sparkExecutorMemory',
  'Spark Executors': 'sparkExecutors',
  'Split By Column': 'splitByColumn',
  'Custom Query': 'customQuery',
  'Sqoop Options': 'sqoopOptions',
  'Use Generated SQL': 'useGeneratedSql',
  'Allow Text Splitter': 'allowTextSplitter',
  'Force String': 'forceString',
  Comment: 'comment',
  'Datalake Source': 'datalakeSource',
  'Operator Notes': 'operatorNotes',
  'Create Foreign Keys': 'createForeignKeys',
  'Invalidate Impala': 'invalidateImpala',
  'Custom Max Query': 'customMaxQuery',
  'Merge Compaction Method': 'mergeCompactionMethod',
  'Source Table Type': 'sourceTableType',
  'Import Database': 'importDatabase',
  'Import Table': 'importTable',
  'History Database': 'historyDatabase',
  'History Table': 'historyTable'
}

export const labelToUITableKey: { [label: string]: keyof UITableWithoutEnum } =
  {
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

export function getKeyFromLabel(
  label: string
): keyof UITableWithoutEnum | undefined {
  return labelToUITableKey[label]
}

export const labelToColumnKey: { [label: string]: keyof Columns } = {
  'Column Name': 'columnName',
  'Column Order': 'columnOrder',
  'Source Column Name': 'sourceColumnName',
  'Column Type': 'columnType',
  'Source Column Type': 'sourceColumnType',
  'Source Database Type': 'sourceDatabaseType',
  'Column Name Override': 'columnNameOverride',
  'Column Type Override': 'columnTypeOverride',
  'Sqoop Column Type': 'sqoopColumnType',
  'Sqoop Column Type Override': 'sqoopColumnTypeOverride',
  'Force String': 'forceString',
  'Include In Import': 'includeInImport',
  'Source Primary Key': 'sourcePrimaryKey',
  'Last Update From Source': 'lastUpdateFromSource',
  Comment: 'comment',
  'Operator Notes': 'operatorNotes',
  'Anonymization Function': 'anonymizationFunction'
}

export function getKeyFromColumnLabel(
  label: string
): keyof Columns | undefined {
  return labelToColumnKey[label]
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
    'Insert only': EtlType.InsertOnly,
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
  },
  'Anonymization Function': {
    None: AnonymizationFunction.None,
    Hash: AnonymizationFunction.Hash,
    'Replace with star': AnonymizationFunction.ReplaceWithStar,
    'Show first 4 chars': AnonymizationFunction.ShowFirst4Chars
  }
}

export const mapDisplayValue = (key: string, value: string): string => {
  return nameDisplayMappings[key]?.[value] || value
}

// Enum mapping

export function mapEnumValue<T extends string>(
  value: string,
  validValues: T[]
): T {
  if (validValues.includes(value as T)) {
    return value as T
  } else {
    throw new Error(`Invalid enum value: ${value}`)
  }
}

export const reverseMapEnumValue = (
  category: string,
  displayValue: string
): string => {
  const categoryMappings = nameReverseMappings[category]

  if (!categoryMappings) {
    throw new Error(`No mappings found for category: ${category}`)
  }

  const backendValue = categoryMappings[displayValue]

  if (!backendValue) {
    throw new Error(
      `No backend value found for display value: '${displayValue}' in category: '${category}'`
    )
  }

  return backendValue
}

export const labelToConnectionKey: { [label: string]: keyof Connection } = {
  Name: 'name',
  'Connection String': 'connectionString',
  'Private Key Path': 'privateKeyPath',
  'Public Key Path': 'publicKeyPath',
  Credentials: 'credentials',
  Source: 'source',
  'Force String': 'forceString',
  'Max Sessions': 'maxSessions',
  'Create Datalake Import': 'createDatalakeImport',
  'Time Window Start': 'timeWindowStart',
  'Time Window Stop': 'timeWindowStop',
  'Time Window Timezone': 'timeWindowTimezone',
  'Operator Notes': 'operatorNotes',
  'Contact Information': 'contactInformation',
  Description: 'description',
  Owner: 'owner',
  Environment: 'environment',
  'Seed File': 'seedFile',
  'Create Foreign Key': 'createForeignKey',
  'Atlas Discovery': 'atlasDiscovery',
  'Atlas Include Filter': 'atlasIncludeFilter',
  'Atlas Exclude Filter': 'atlasExcludeFilter',
  'Atlas Last Discovery': 'atlasLastDiscovery'
}

export function getKeyFromConnectionLabel(
  label: string
): keyof Connection | undefined {
  return labelToConnectionKey[label]
}

// Airflow

export const labelToImportAirflowKey: {
  [label: string]: keyof ImportAirflowDAG
} = {
  'DAG Name': 'name',
  'Schedule Interval': 'scheduleInterval',
  Retries: 'retries',
  'Operator Notes': 'operatorNotes',
  'Application Notes': 'applicationNotes',
  'Auto Regenerate DAG': 'autoRegenerateDag',
  'Airflow Notes': 'airflowNotes',
  'Sudo User': 'sudoUser',
  Timezone: 'timezone',
  Email: 'email',
  'Email On Failure': 'emailOnFailure',
  'Email On Retries': 'emailOnRetries',
  Tags: 'tags',
  'Sla Warning Time': 'slaWarningTime',
  'Retry Exponential Backoff': 'retryExponentialBackoff',
  Concurrency: 'concurrency',

  'Filter Table': 'filterTable',
  'Finish all Stage 1 first': 'finishAllStage1First',
  'Run Import and Etl separate': 'runImportAndEtlSeparate',
  'Retries Stage 1': 'retriesStage1',
  'Retries Stage 2': 'retriesStage2',
  'Pool Stage 1': 'poolStage1',
  'Pool Stage 2': 'poolStage2',
  'Run Only Metadata Import': 'metadataImport'
}

export const labelToExportAirflowKey: {
  [label: string]: keyof ExportAirflowDAG
} = {
  'DAG Name': 'name',
  'Schedule Interval': 'scheduleInterval',
  Retries: 'retries',
  'Operator Notes': 'operatorNotes',
  'Application Notes': 'applicationNotes',
  'Auto Regenerate DAG': 'autoRegenerateDag',
  'Airflow Notes': 'airflowNotes',
  'Sudo User': 'sudoUser',
  Timezone: 'timezone',
  Email: 'email',
  'Email On Failure': 'emailOnFailure',
  'Email On Retries': 'emailOnRetries',
  Tags: 'tags',
  'Sla Warning Time': 'slaWarningTime',
  'Retry Exponential Backoff': 'retryExponentialBackoff',
  Concurrency: 'concurrency',

  'Filter Connection': 'filterConnection',
  'Filter Target Schema': 'filterTargetSchema',
  'Filter Target Table': 'filterTargetTable'
}

export const labelToCustomAirflowKey: {
  [label: string]: keyof CustomAirflowDAG
} = {
  'DAG Name': 'name',
  'Schedule Interval': 'scheduleInterval',
  Retries: 'retries',
  'Operator Notes': 'operatorNotes',
  'Application Notes': 'applicationNotes',
  'Auto Regenerate DAG': 'autoRegenerateDag',
  'Airflow Notes': 'airflowNotes',
  'Sudo User': 'sudoUser',
  Timezone: 'timezone',
  Email: 'email',
  'Email On Failure': 'emailOnFailure',
  'Email On Retries': 'emailOnRetries',
  Tags: 'tags',
  'Sla Warning Time': 'slaWarningTime',
  'Retry Exponential Backoff': 'retryExponentialBackoff',
  Concurrency: 'concurrency'
}

export function getKeyFromImportAirflowLabel(
  label: string
): keyof WithDynamicKeys<ImportAirflowDAG> | undefined {
  return labelToImportAirflowKey[label]
}

export function getKeyFromExportAirflowLabel(
  label: string
): keyof WithDynamicKeys<ExportAirflowDAG> | undefined {
  return labelToExportAirflowKey[label]
}

export function getKeyFromCustomAirflowLabel(
  label: string
): keyof WithDynamicKeys<CustomAirflowDAG> | undefined {
  return labelToCustomAirflowKey[label]
}
