import { EtlEngine, EtlType, ImportTool, ImportType } from './enums'
import {
  UITable,
  TableSetting,
  TableUpdate,
  TableCreateWithoutEnum
} from './interfaces'
import {
  getKeyFromColumnLabel,
  getKeyFromLabel,
  reverseMapEnumValue
} from './nameMappings'

const fieldsToRemove = [
  'sourceRowcount',
  'sourceRowcountIncr',
  'targetRowcount',
  'validationCustomQueryHiveValue',
  'validationCustomQuerySourceValue',
  'incrMinvalue',
  'incrMaxvalue',
  'incrMinvaluePending',
  'incrMaxvaluePending',
  'lastSize',
  'lastRows',
  'lastMappers',
  'lastExecution',
  'generatedHiveColumnDefinition',
  'generatedSqoopQuery',
  'generatedSqoopOptions',
  'generatedPkColumns',
  'generatedForeignKeys',
  'copyFinished',
  'copySlave'
]

function updateColumnData(
  tableData: UITable,
  setting: TableSetting,
  indexInColumns: number,
  columnKey: string
) {
  if (!columnKey || !tableData.columns) return

  if (
    typeof indexInColumns !== 'number' ||
    indexInColumns < 0 ||
    indexInColumns >= tableData.columns.length
  ) {
    console.error('Invalid column index:', indexInColumns)
    return
  }

  tableData.columns[indexInColumns][columnKey] = setting.value
}

export function updateTableData(
  tableData: UITable,
  updatedSettings: TableSetting[],
  column?: boolean
): TableUpdate {
  const updatedTableData = { ...tableData }

  updatedSettings.forEach((setting) => {
    const key = column
      ? getKeyFromColumnLabel(setting.label)
      : getKeyFromLabel(setting.label)
    if (key) {
      let value = setting.value

      if (setting.type === 'enum' && setting.enumOptions) {
        value = reverseMapEnumValue(setting.label, value as string)
      }
      if (column === true) {
        const indexInColumnsSetting = updatedSettings.find(
          (setting) => setting.label === 'Column Order'
        )
        const indexInColumns = (indexInColumnsSetting?.value as number) - 1
        updateColumnData(updatedTableData, setting, indexInColumns, key)
      } else {
        updatedTableData[key] = value
      }
    }
  })

  const finalTableData = Object.keys(updatedTableData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedTableData[key]
    }
    return acc
  }, {} as TableUpdate)

  return finalTableData
}

export function createTableData(
  newTableSettings: TableSetting[]
): TableCreateWithoutEnum {
  // Part 1: Keys that will get values from newTableSettings
  const part1: {
    database: string
    table: string
    connection: string
    sourceSchema: string
    sourceTable: string
    importPhaseType: string
    etlPhaseType: string
    importTool: string
    etlEngine: string
  } = {
    database: '',
    table: '',
    connection: '',
    sourceSchema: '',
    sourceTable: '',
    importPhaseType: ImportType.Full,
    etlPhaseType: EtlType.TruncateAndInsert,
    importTool: ImportTool.Spark,
    etlEngine: EtlEngine.Spark
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<TableCreateWithoutEnum, keyof typeof part1> = {
    lastUpdateFromSource: null,
    sqlWhereAddition: null,
    nomergeIngestionSqlAddition: null,
    includeInAirflow: null,
    airflowPriority: null,
    validateImport: null,
    validationMethod: null,
    validateSource: null,
    validateDiffAllowed: null,
    validationCustomQuerySourceSQL: null,
    validationCustomQueryHiveSQL: null,
    validationCustomQueryValidateImportTable: null,
    truncateTable: null,
    mappers: null,
    softDeleteDuringMerge: null,
    incrMode: null,
    incrColumn: null,
    incrValidationMethod: null,
    pkColumnOverride: null,
    pkColumnOverrideMergeonly: null,
    mergeHeap: null,
    splitCount: null,
    sparkExecutorMemory: null,
    sparkExecutors: null,
    splitByColumn: null,
    customQuery: null,
    sqoopOptions: null,
    useGeneratedSql: null,
    allowTextSplitter: null,
    forceString: null,
    comment: null,
    datalakeSource: null,
    operatorNotes: null,
    createForeignKeys: null,
    invalidateImpala: null,
    customMaxQuery: null,
    mergeCompactionMethod: null,
    sourceTableType: null,
    importDatabase: null,
    importTable: null,
    historyDatabase: null,
    historyTable: null,
    columns: [] // Initialize columns explicitly as an empty array
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    Database: 'database',
    Table: 'table',
    Connection: 'connection',
    'Source Schema': 'sourceSchema',
    'Source Table': 'sourceTable',
    'Import Type': 'importPhaseType',
    'ETL Type': 'etlPhaseType',
    'Import Tool': 'importTool',
    'ETL Engine': 'etlEngine'
  }

  const filteredSettings = newTableSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    let value = setting.value

    if (setting.type === 'enum' && setting.enumOptions) {
      value = reverseMapEnumValue(setting.label, value as string)
    }
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean'
      ) {
        part1[key] = value as (typeof part1)[typeof key]
      }
    }
  })

  const finalCreateTableData: TableCreateWithoutEnum = { ...part2, ...part1 }
  // const finalCreateTableData: Omit<TableCreateMapped, 'includeInAirflow'> = { ...part2, ...part1 };

  return finalCreateTableData
}
