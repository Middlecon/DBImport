import {
  AnonymizationFunction,
  EtlEngine,
  EtlType,
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
  truncateTable: boolean
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
  generatedForeignKeys: string
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
  truncateTable: boolean
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
  generatedForeignKeys: string
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
  truncateTable: boolean
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
  generatedForeignKeys: string
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
  truncateTable: boolean
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
  truncateTable: null
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
  truncateTable: null
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

export type TableSettingsValueTypes = string | number | boolean | EnumTypes

export interface TableSetting {
  label: string
  value: TableSettingsValueTypes | null
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: {
    [key: string]: string
  }
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
