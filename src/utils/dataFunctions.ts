import { UITable, TableSetting, TableCreateUpdate } from './interfaces'
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
): TableCreateUpdate {
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
  }, {} as TableCreateUpdate)

  return finalTableData
}
