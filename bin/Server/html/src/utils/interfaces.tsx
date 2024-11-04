import {
  AirflowDAGTaskPlacement,
  AirflowDAGTaskType,
  AnonymizationFunction,
  EtlEngine,
  EtlType,
  ExportIncrValidationMethod,
  ExportTool,
  ExportType,
  ExportValidationMethod,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  SettingType,
  ValidateSource,
  ValidationMethod
} from './enums'

export interface ConnectionNames {
  name: string
}
export interface Connections extends ConnectionNames {
  connectionString: string
  serverType: string
}

export interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
}

export interface Connection {
  name: string | null
  connectionString: string
  privateKeyPath: string | null
  publicKeyPath: string | null
  credentials: string | null
  source: string | null
  forceString: number
  maxSessions: number | null
  createDatalakeImport: boolean
  timeWindowStart: string | null
  timeWindowStop: string | null
  timeWindowTimezone: string | null
  operatorNotes: string | null
  contactInformation: string | null
  description: string | null
  owner: string | null
  environment: string | null
  seedFile: string | null
  createForeignKey: boolean
  atlasDiscovery: boolean
  atlasIncludeFilter: string | null
  atlasExcludeFilter: string | null
  atlasLastDiscovery: string | null
}

// Table

export interface DbTable {
  connection: string
  database: string
  etlEngine: string
  etlPhaseType: string
  importPhaseType: string
  importTool: string
  lastUpdateFromSource: string
  sourceSchema: string
  sourceTable: string
  table: string
}

export interface UiDbTable extends Table {
  importPhaseTypeDisplay: string
  etlPhaseTypeDisplay: string
  importToolDisplay: string
  etlEngineDisplay: string
}

export interface Table {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: string
  validateSource: string
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean
  mappers: 0
  softDeleteDuringMerge: boolean
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: string
  incrColumn: string
  incrValidationMethod: string
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  mergeHeap: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  customQuery: string
  sqoopOptions: string
  lastSize: 0
  lastRows: 0
  lastMappers: 0
  lastExecution: 0
  useGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: 0
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: string
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface UITable {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: ValidationMethod
  validateSource: ValidateSource
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  mappers: 0
  softDeleteDuringMerge: boolean
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: IncrMode
  incrColumn: string
  incrValidationMethod: IncrValidationMethod
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  mergeHeap: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  customQuery: string
  sqoopOptions: string
  lastSize: 0
  lastRows: 0
  lastMappers: 0
  lastExecution: 0
  useGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: 0
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: MergeCompactionMethod
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface UITableWithoutEnum {
  [key: string]: string | boolean | number | Columns[]
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: string
  validateSource: string
  validateDiffAllowed: number
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  mappers: number
  softDeleteDuringMerge: boolean
  sourceRowcount: number
  sourceRowcountIncr: number
  targetRowcount: number
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: string
  incrColumn: string
  incrValidationMethod: string
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  mergeHeap: number
  splitCount: number
  sparkExecutorMemory: string
  sparkExecutors: number
  splitByColumn: string
  customQuery: string
  sqoopOptions: string
  lastSize: number
  lastRows: number
  lastMappers: number
  lastExecution: number
  useGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: number
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: number
  invalidateImpala: number
  customMaxQuery: string
  mergeCompactionMethod: string
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface TableUpdate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: ValidationMethod
  validateSource: ValidateSource
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  mappers: 0
  softDeleteDuringMerge: boolean
  incrMode: IncrMode
  incrColumn: string
  incrValidationMethod: IncrValidationMethod
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  mergeHeap: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  customQuery: string
  sqoopOptions: string
  useGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: number
  comment: string
  datalakeSource: string
  operatorNotes: string
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: MergeCompactionMethod
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface TableCreateWithoutEnum {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: null
  sqlWhereAddition: null
  nomergeIngestionSqlAddition: null
  includeInAirflow: null
  airflowPriority: null
  validateImport: null
  validationMethod: null
  validateSource: null
  validateDiffAllowed: null
  validationCustomQuerySourceSQL: null
  validationCustomQueryHiveSQL: null
  validationCustomQueryValidateImportTable: null
  mappers: null
  softDeleteDuringMerge: null
  incrMode: null
  incrColumn: null
  incrValidationMethod: null
  pkColumnOverride: null
  pkColumnOverrideMergeonly: null
  mergeHeap: null
  splitCount: null
  sparkExecutorMemory: null
  sparkExecutors: null
  splitByColumn: null
  customQuery: null
  sqoopOptions: null
  useGeneratedSql: null
  allowTextSplitter: null
  forceString: null
  comment: null
  datalakeSource: null
  operatorNotes: null
  createForeignKeys: null
  invalidateImpala: null
  customMaxQuery: null
  mergeCompactionMethod: null
  sourceTableType: null
  importDatabase: null
  importTable: null
  historyDatabase: null
  historyTable: null
  columns: []
}

export interface TableCreate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: null
  sqlWhereAddition: null
  nomergeIngestionSqlAddition: null
  includeInAirflow: null
  airflowPriority: null
  validateImport: null
  validationMethod: null
  validateSource: null
  validateDiffAllowed: null
  validationCustomQuerySourceSQL: null
  validationCustomQueryHiveSQL: null
  validationCustomQueryValidateImportTable: null
  mappers: null
  softDeleteDuringMerge: null
  incrMode: null
  incrColumn: null
  incrValidationMethod: null
  pkColumnOverride: null
  pkColumnOverrideMergeonly: null
  mergeHeap: null
  splitCount: null
  sparkExecutorMemory: null
  sparkExecutors: null
  splitByColumn: null
  customQuery: null
  sqoopOptions: null
  useGeneratedSql: null
  allowTextSplitter: null
  forceString: null
  comment: null
  datalakeSource: null
  operatorNotes: null
  createForeignKeys: null
  invalidateImpala: null
  customMaxQuery: null
  mergeCompactionMethod: null
  sourceTableType: null
  importDatabase: null
  importTable: null
  historyDatabase: null
  historyTable: null
  columns: Columns[]
}

export type TableCreateMapped = {
  [K in keyof Omit<TableCreate, 'columns'>]: TableCreate[K]
} & {
  columns: Columns[]
}

export interface Column<T> {
  header: string
  accessor?: keyof T
  isAction?: 'edit' | 'delete' | 'both'
}

// Import table Columns, it is different on Export
export interface Columns {
  columnName: string
  columnOrder: string | null
  sourceColumnName: string
  columnType: string
  sourceColumnType: string
  sourceDatabaseType: string | null
  columnNameOverride: string | null
  columnTypeOverride: string | null
  sqoopColumnType: string | null
  sqoopColumnTypeOverride: string | null
  forceString: number
  includeInImport: string
  sourcePrimaryKey: string | null
  lastUpdateFromSource: string
  comment: string | null
  operatorNotes: string | null
  anonymizationFunction: string
}

export type EnumTypes =
  | ImportType
  | EtlType
  | ImportTool
  | EtlEngine
  | ValidationMethod
  | ValidateSource
  | IncrMode
  | IncrValidationMethod
  | MergeCompactionMethod
  | AnonymizationFunction
  | AirflowDAGTaskType
  | AirflowDAGTaskPlacement
  | ExportType
  | ExportTool

export type EditSettingValueTypes = string | number | boolean | EnumTypes

export interface EditSetting {
  label: string
  value: EditSettingValueTypes | null
  type: SettingType
  enumOptions?: {
    [key: string]: string
  }
  infoText?: string
  isConditionsMet?: boolean
  isHidden?: boolean
}

export interface TableCreate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
}

// Export

export interface ExportConnections {
  name: string
  tables: number
  lastExport: string | null
  lastRows: number | null
}

export interface ExportCnTablesWithoutEnum {
  connection: string
  targetSchema: string
  targetTable: string
  database: string
  table: string
  exportType: string
  exportTool: string
  lastUpdateFromHive: string | null
}

export interface ExportCnTables {
  connection: string
  targetSchema: string
  targetTable: string
  database: string
  table: string
  exportType: ExportType
  exportTool: ExportTool
  lastUpdateFromHive: string | null
}
export interface UIExportCnTables extends ExportCnTablesWithoutEnum {
  exportTypeDisplay: string
  exportToolDisplay: string
}

export interface ExportTable {
  connection: string
  targetSchema: string
  targetTable: string
  exportType: ExportType
  exportTool: ExportTool
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: string | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  mappers: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: string | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastMappers: number | null
  lastExecution: number | null
  javaHeap: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface ExportColumns {
  columnName: string
  columnType: string
  columnOrder: number | null
  targetColumnName: string | null
  targetColumnType: string | null
  lastUpdateFromHive: string | null
  includeInExport: boolean
  comment: string | null
  operatorNotes: string | null
}

export interface UIExportTable {
  connection: string
  targetSchema: string
  targetTable: string
  exportType: ExportType
  exportTool: ExportTool
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: ExportValidationMethod | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  mappers: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: ExportIncrValidationMethod | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastMappers: number | null
  lastExecution: number | null
  javaHeap: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface UIExportTableWithoutEnum {
  [key: string]: string | boolean | number | null | ExportColumns[]
  connection: string
  targetSchema: string
  targetTable: string
  exportType: string
  exportTool: string
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: string | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  mappers: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: string | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastMappers: number | null
  lastExecution: number | null
  javaHeap: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

// Airflow

export interface BaseAirflowsData {
  name: string
  scheduleInterval: string
  autoRegenerateDag: boolean
}

export interface AirflowsData extends BaseAirflowsData {
  type: string
  operatorNotes: string
  applicationNotes: string
}

export interface AirflowsImportData extends BaseAirflowsData {
  filterTable: string
}

export interface UiAirflowsImportData extends AirflowsImportData {
  autoRegenerateDagDisplay: string
}

export interface AirflowsExportData extends BaseAirflowsData {
  filterConnection: string
  filterTargetSchema: string
  filterTargetTable: string
}

export interface UiAirflowsExportData extends AirflowsExportData {
  autoRegenerateDagDisplay: string
}

export type AirflowsCustomData = BaseAirflowsData

export interface UiAirflowsCustomData extends AirflowsCustomData {
  autoRegenerateDagDisplay: string
}

export type AirflowWithDynamicKeys<T> = T & {
  [key: string]: string | boolean | number | AirflowTask[]
}

export interface BaseAirflowDAG {
  name: string
  scheduleInterval: string
  retries: number
  operatorNotes: string
  applicationNotes: string
  autoRegenerateDag: boolean
  airflowNotes: string
  sudoUser: string
  timezone: string
  email: string
  emailOnFailure: boolean
  emailOnRetries: boolean
  tags: string
  slaWarningTime: string
  retryExponentialBackoff: boolean
  concurrency: number
  tasks: AirflowTask[]
}

export interface ImportAirflowDAG extends BaseAirflowDAG {
  filterTable: string
  finishAllStage1First: boolean
  runImportAndEtlSeparate: boolean
  retriesStage1: number
  retriesStage2: number
  poolStage1: string
  poolStage2: string
  metadataImport: boolean
}

export interface ExportAirflowDAG extends BaseAirflowDAG {
  filterConnection: string
  filterTargetSchema: string
  filterTargetTable: string
}

export type CustomAirflowDAG = BaseAirflowDAG

export interface AirflowTask {
  name: string
  type: string
  placement: string
  connection: string | null
  airflowPool: string | null
  airflowPriority: number | null
  includeInAirflow: boolean
  taskDependencyDownstream: string | null
  taskDependencyUpstream: string | null
  taskConfig: string | null
  sensorPokeInterval: number | null
  sensorTimeoutMinutes: number | null
  sensorConnection: string | null
  sensorSoftFail: number | null
  sudoUser: string | null
}

export interface BaseCreateAirflowDAG {
  name: string
  scheduleInterval: string | null
  retries: number | null
  operatorNotes: string | null
  applicationNotes: string | null
  autoRegenerateDag: boolean
  airflowNotes: string | null
  sudoUser: string | null
  timezone: string | null
  email: string | null
  emailOnFailure: boolean | null
  emailOnRetries: boolean | null
  tags: string | null
  slaWarningTime: string | null
  retryExponentialBackoff: boolean | null
  concurrency: number | null
  tasks: AirflowTask[]
}

export interface ImportCreateAirflowDAG extends BaseCreateAirflowDAG {
  filterTable: string | null
  finishAllStage1First: boolean | null
  runImportAndEtlSeparate: boolean | null
  retriesStage1: number | null
  retriesStage2: number | null
  poolStage1: string | null
  poolStage2: string | null
  metadataImport: boolean | null
}

export interface ExportCreateAirflowDAG extends BaseCreateAirflowDAG {
  filterConnection: string | null
  filterTargetSchema: string | null
  filterTargetTable: string | null
}

export type CustomCreateAirflowDAG = BaseCreateAirflowDAG

// export interface BaseCreateAirflowDAG {
//   name: string
//   scheduleInterval: string | null
//   retries: number
//   operatorNotes: string | null
//   applicationNotes: string | null
//   autoRegenerateDag: boolean
//   airflowNotes: string | null
//   sudoUser: string | null
//   timezone: string | null
//   email: string | null
//   emailOnFailure: boolean
//   emailOnRetries: boolean
//   tags: string | null
//   slaWarningTime: string | null
//   retryExponentialBackoff: boolean
//   concurrency: number | null
//   tasks: AirflowTask[]
// }

// export interface ImportCreateAirflowDAG extends BaseCreateAirflowDAG {
//   filterTable: string | null
//   finishAllStage1First: boolean | null
//   runImportAndEtlSeparate: boolean
//   retriesStage1: number | null
//   retriesStage2: number | null
//   poolStage1: string | null
//   poolStage2: string | null
//   metadataImport: boolean
// }

// export interface ExportCreateAirflowDAG extends BaseCreateAirflowDAG {
//   filterConnection: string
//   filterTargetSchema: string | null
//   filterTargetTable: string | null
// }

// export type CustomCreateAirflowDAG = BaseCreateAirflowDAG
