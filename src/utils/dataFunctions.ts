import { UITable, TableSetting, TableUpdate } from './interfaces'
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
  // console.log(
  //   'Updating column at index:',
  //   indexInColumns,
  //   'with key:',
  //   columnKey,
  //   'and value:',
  //   setting.value
  // )
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
    // console.log('setting.label', setting.label)
    const key = column
      ? getKeyFromColumnLabel(setting.label)
      : getKeyFromLabel(setting.label)
    if (key) {
      // console.log('key', key)

      let value = setting.value

      if (setting.type === 'enum' && setting.enumOptions) {
        value = reverseMapEnumValue(setting.label, value as string)
      }
      if (column === true) {
        const indexInColumnsSetting = updatedSettings.find(
          (setting) => setting.label === 'Column Order'
        )
        // console.log('indexInColumnsSetting', indexInColumnsSetting)
        const indexInColumns = (indexInColumnsSetting?.value as number) - 1
        // console.log('GOT IN to column in updateTableData')
        updateColumnData(updatedTableData, setting, indexInColumns, key)
      } else {
        updatedTableData[key] = value
      }
      // updatedTableData[key] = value
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
