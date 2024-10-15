import { EtlEngine, EtlType, ImportTool, ImportType } from './enums'
import {
  UITable,
  TableSetting,
  TableCreateWithoutEnum,
  Connection,
  Columns,
  UITableWithoutEnum
} from './interfaces'
import {
  // getKeyFromColumnLabel,
  // getKeyFromConnectionLabel,
  getKeyFromLabel,
  reverseMapEnumValue
} from './nameMappings'

// Connection

export function updateConnectionData(
  originalData: Connection,
  updatedSettings: TableSetting[]
): Connection {
  // Part 1: Keys that will get values from updatedSettings
  const part1: {
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
    seedFile: string | null
    createForeignKey: boolean
    atlasDiscovery: boolean
    atlasIncludeFilter: string | null
    atlasExcludeFilter: string | null
    atlasLastDiscovery: string | null
  } = {
    connectionString: originalData.connectionString,
    privateKeyPath: originalData.privateKeyPath,
    publicKeyPath: originalData.publicKeyPath,
    credentials: originalData.credentials,
    source: originalData.source,
    forceString: originalData.forceString,
    maxSessions: originalData.maxSessions,
    createDatalakeImport: originalData.createDatalakeImport,
    timeWindowStart: originalData.timeWindowStart,
    timeWindowStop: originalData.timeWindowStop,
    timeWindowTimezone: originalData.timeWindowTimezone,
    operatorNotes: originalData.operatorNotes,
    contactInformation: originalData.contactInformation,
    description: originalData.description,
    owner: originalData.owner,
    seedFile: originalData.seedFile,
    createForeignKey: originalData.createForeignKey,
    atlasDiscovery: originalData.atlasDiscovery,
    atlasIncludeFilter: originalData.atlasIncludeFilter,
    atlasExcludeFilter: originalData.atlasExcludeFilter,
    atlasLastDiscovery: originalData.atlasLastDiscovery
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<Connection, keyof typeof part1> = {
    name: originalData.name,
    environment: originalData.environment
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
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
    'Seed File': 'seedFile',
    'Create Foreign Key': 'createForeignKey',
    'Atlas Discovery': 'atlasDiscovery',
    'Atlas Include Filter': 'atlasIncludeFilter',
    'Atlas Exclude Filter': 'atlasExcludeFilter',
    'Atlas Last Discovery': 'atlasLastDiscovery'
  }

  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    let value = setting.value

    if (setting.type === 'enum' && setting.enumOptions) {
      value = reverseMapEnumValue(setting.label, value as string)
    }
    // To do: use getKeyFromConnectionLabel instead of labelToKeyMap if that is better
    // const key = getKeyFromConnectionLabel(setting.label)
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean' ||
        value === null
      ) {
        ;(part1[key] as (typeof part1)[typeof key]) = value
      }
    }
  })

  if (part1.credentials === '') {
    part1.credentials = null
  }

  // To do: make generic conversion of empty string to null, WIP:
  // Object.keys(part1).forEach((key) => {
  //   const typedKey = key as keyof typeof part1;
  //   type CanBeNull = null extends typeof part1[typeof typedKey] ? true : false;

  //   if (part1[typedKey] === ''  && (true as CanBeNull)) {
  //     part1[typedKey] = null;
  //   }
  // });

  const finalConnectionData: Connection = { ...part2, ...part1 }

  return finalConnectionData
}

// Table

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
  tableData: UITableWithoutEnum,
  setting: TableSetting,
  indexInColumns: number
) {
  if (!tableData.columns) return

  const currentColumn = tableData.columns[indexInColumns]

  const part1: {
    columnNameOverride: string | null
    columnTypeOverride: string | null
    sqoopColumnTypeOverride: string | null
    forceString: number
    includeInImport: string
    comment: string | null
    operatorNotes: string | null
    anonymizationFunction: string
  } = {
    columnNameOverride: currentColumn.columnNameOverride,
    columnTypeOverride: currentColumn.columnTypeOverride,
    sqoopColumnTypeOverride: currentColumn.sqoopColumnTypeOverride,
    forceString: currentColumn.forceString,
    includeInImport: currentColumn.includeInImport,
    comment: currentColumn.comment,
    operatorNotes: currentColumn.operatorNotes,
    anonymizationFunction: currentColumn.anonymizationFunction
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<Columns, keyof typeof part1> = {
    columnName: currentColumn.columnName,
    columnOrder: currentColumn.columnOrder,
    sourceColumnName: currentColumn.sourceColumnName,
    columnType: currentColumn.columnType,
    sourceColumnType: currentColumn.sourceColumnType,
    sourceDatabaseType: currentColumn.sourceDatabaseType,
    sqoopColumnType: currentColumn.sqoopColumnType,
    sourcePrimaryKey: currentColumn.sourcePrimaryKey,
    lastUpdateFromSource: currentColumn.lastUpdateFromSource
  }

  const labelToColumnMap: Record<string, keyof typeof part1> = {
    'Column Name Override': 'columnNameOverride',
    'Column Type Override': 'columnTypeOverride',
    'Sqoop Column Type Override': 'sqoopColumnTypeOverride',
    'Force String': 'forceString',
    'Include In Import': 'includeInImport',
    Comment: 'comment',
    'Operator Notes': 'operatorNotes',
    'Anonymization Function': 'anonymizationFunction'
  }

  const key = labelToColumnMap[setting.label]

  const settingValue = setting.value

  if (key) {
    if (
      typeof settingValue === 'string' ||
      typeof settingValue === 'number' ||
      settingValue === null
    ) {
      ;(part1[key] as (typeof part1)[typeof key]) = settingValue
    }
  }

  const finalColumnData: Columns = { ...part2, ...part1 }
  console.log('finalColumnData', finalColumnData)
  tableData.columns[indexInColumns] = finalColumnData
}

export function updateTableData(
  tableData: UITable,
  updatedSettings: TableSetting[],
  column?: boolean
): UITableWithoutEnum {
  const updatedTableData: UITableWithoutEnum = {
    ...tableData
  } as UITableWithoutEnum
  console.log('typeof updatedTableData', typeof updatedTableData)
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    if (column === true) {
      const indexInColumnsSetting = updatedSettings.find(
        (setting) => setting.label === 'Column Order'
      )
      const indexInColumns = (indexInColumnsSetting?.value as number) - 1
      updateColumnData(updatedTableData, setting, indexInColumns)
    } else {
      let value = setting.value as string

      if (setting.type === 'enum' && setting.enumOptions) {
        value = reverseMapEnumValue(setting.label, value as string)
      }

      const key = getKeyFromLabel(setting.label)

      if (key) {
        console.log('updatedTableData[key]', updatedTableData[key])
        console.log(
          'typeof updatedTableData[key]',
          typeof updatedTableData[key]
        )
        updatedTableData[key] = value
      }
    }
  })

  const finalTableData = Object.keys(updatedTableData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedTableData[key]
    }
    return acc
  }, {} as UITableWithoutEnum)

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
