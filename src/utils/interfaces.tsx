import {
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

export interface Connection {
  name: 'string'
}

export interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
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

export interface Column<T> {
  header: string
  accessor?: keyof T
  isAction?: 'edit' | 'delete' | 'both'
}

export interface Table {
  database: 'string'
  table: 'string'
  connection: 'string'
  sourceSchema: 'string'
  sourceTable: 'string'
  importPhaseType: 'string'
  etlPhaseType: 'string'
  importTool: 'string'
  etlEngine: 'string'
  lastUpdateFromSource: 'string'
  sqlWhereAddition: 'string'
  nomergeIngestionSqlAddition: 'string'
  includeInAirflow: true
  airflowPriority: 'string'
  validateImport: true
  validationMethod: 'string'
  validateSource: 'string'
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: 'string'
  validationCustomQueryHiveSQL: 'string'
  validationCustomQueryValidateImportTable: true
  truncateTable: true
  mappers: 0
  softDeleteDuringMerge: true
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: 'string'
  validationCustomQueryHiveValue: 'string'
  incrMode: 'string'
  incrColumn: 'string'
  incrValidationMethod: 'string'
  incrMinvalue: 'string'
  incrMaxvalue: 'string'
  incrMinvaluePending: 'string'
  incrMaxvaluePending: 'string'
  pkColumnOverride: 'string'
  pkColumnOverrideMergeonly: 'string'
  mergeHeap: 0
  splitCount: 0
  sparkExecutorMemory: 'string'
  sparkExecutors: 0
  splitByColumn: 'string'
  customQuery: 'string'
  sqoopOptions: 'string'
  lastSize: 0
  lastRows: 0
  lastMappers: 0
  lastExecution: 0
  useGeneratedSql: true
  allowTextSplitter: true
  forceString: 0
  comment: 'string'
  generatedHiveColumnDefinition: 'string'
  generatedSqoopQuery: 'string'
  generatedSqoopOptions: 'string'
  generatedPkColumns: 'string'
  generatedForeignKeys: 'string'
  datalakeSource: 'string'
  operatorNotes: 'string'
  copyFinished: 'string'
  copySlave: true
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: 'string'
  mergeCompactionMethod: 'string'
  sourceTableType: 'string'
  importDatabase: 'string'
  importTable: 'string'
  historyDatabase: 'string'
  historyTable: 'string'
  columns: []
}

export interface UITable {
  database: 'string'
  table: 'string'
  connection: 'string'
  sourceSchema: 'string'
  sourceTable: 'string'
  importPhaseType: ImportType | 'Unknown'
  etlPhaseType: EtlType | 'Unknown'
  importTool: ImportTool | 'Unknown'
  etlEngine: EtlEngine | 'Unknown'
  lastUpdateFromSource: 'string'
  sqlWhereAddition: 'string'
  nomergeIngestionSqlAddition: 'string'
  includeInAirflow: boolean
  airflowPriority: 'string'
  validateImport: boolean
  validationMethod: ValidationMethod | 'Unknown'
  validateSource: ValidateSource | 'Unknown'
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: 'string'
  validationCustomQueryHiveSQL: 'string'
  validationCustomQueryValidateImportTable: true // should stay true
  truncateTable: boolean
  mappers: 0
  softDeleteDuringMerge: boolean
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: 'string'
  validationCustomQueryHiveValue: 'string'
  incrMode: IncrMode | 'Unknown'
  incrColumn: 'string'
  incrValidationMethod: IncrValidationMethod | 'Unknown'
  incrMinvalue: 'string'
  incrMaxvalue: 'string'
  incrMinvaluePending: 'string'
  incrMaxvaluePending: 'string'
  pkColumnOverride: 'string'
  pkColumnOverrideMergeonly: 'string'
  mergeHeap: 0
  splitCount: 0
  sparkExecutorMemory: 'string'
  sparkExecutors: 0
  splitByColumn: 'string'
  customQuery: 'string'
  sqoopOptions: 'string'
  lastSize: 0
  lastRows: 0
  lastMappers: 0
  lastExecution: 0
  useGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: 0
  comment: 'string'
  generatedHiveColumnDefinition: 'string'
  generatedSqoopQuery: 'string'
  generatedSqoopOptions: 'string'
  generatedPkColumns: 'string'
  generatedForeignKeys: 'string'
  datalakeSource: 'string'
  operatorNotes: 'string'
  copyFinished: 'string'
  copySlave: boolean
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: 'string'
  mergeCompactionMethod: MergeCompactionMethod | 'Unknown'
  sourceTableType: 'string'
  importDatabase: 'string'
  importTable: 'string'
  historyDatabase: 'string'
  historyTable: 'string'
  columns: []
}

export interface Columns {
  columnName: 'string'
  columnOrder: 'string'
  sourceColumnName: 'string'
  columnType: 'string'
  sourceColumnType: 'string'
  sourceDatabaseType: 'string'
  columnNameOverride: 'string'
  columnTypeOverride: 'string'
  sqoopColumnType: 'string'
  sqoopColumnTypeOverride: 'string'
  forceString: 'string'
  includeInImport: 'string'
  sourcePrimaryKey: 'string'
  lastUpdateFromSource: 'string'
  comment: 'string'
  operatorNotes: 'string'
  anonymizationFunction: 'string'
}

export interface TableSetting {
  label: string
  value: string | number | boolean
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string } // Maybe not needed here
  isHidden?: boolean
}
