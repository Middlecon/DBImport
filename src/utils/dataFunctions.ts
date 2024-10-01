import { UITable, TableSetting, TableUpdate } from './interfaces'
import { getKeyFromLabel, reverseMapEnumValue } from './nameMappings'

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

export function updateTableData(
  tableData: UITable,
  newSettings: TableSetting[]
): TableUpdate {
  const updatedTableData = { ...tableData }

  newSettings.forEach((setting) => {
    const key = getKeyFromLabel(setting.label)
    if (key) {
      let value = setting.value

      // Handle enum values; assuming enums have a specific type field and possibly enumOptions
      if (setting.type === 'enum' && setting.enumOptions) {
        // This assumes enumOptions maps display names to enum keys
        value = reverseMapEnumValue(setting.label, value as string)
      }

      updatedTableData[key] = value
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
