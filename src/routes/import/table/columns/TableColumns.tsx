import { Column, Columns, TableSetting } from '../../../../utils/interfaces'
import TableList from '../../../../components/TableList'
import { useState } from 'react'
import { useTable } from '../../../../utils/queries'
import EditTableModal from '../../../../components/EditTableModal'
import { ImportTool, SettingType } from '../../../../utils/enums'
import { nameDisplayMappings } from '../../../../utils/nameMappings'
import { updateTableData } from '../../../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'

function TableColumns() {
  const { data: table, isFetching } = useTable()
  const { table: tableParam } = useParams<{ table: string }>()
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const [isModalOpen, setModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<TableSetting[] | []>([])
  const getEnumOptions = (key: string) => nameDisplayMappings[key] || {}

  if (isFetching) return <div>Loading...</div>
  if (!table) return <div>No data found.</div>

  console.log('tableData', table)

  const columnsData = table.columns

  const columns: Column<Columns>[] = [
    { header: 'Column Name', accessor: 'columnName' },
    { header: 'Column Order', accessor: 'columnOrder' },
    { header: 'Source Column Name', accessor: 'sourceColumnName' },
    { header: 'Column Type', accessor: 'columnType' },
    { header: 'Source Column Type', accessor: 'sourceColumnType' },
    { header: 'Source Database Type', accessor: 'sourceDatabaseType' },
    { header: 'Column Name Override', accessor: 'columnNameOverride' },
    { header: 'Column Type Override', accessor: 'columnTypeOverride' },
    { header: 'Sqoop Column Type', accessor: 'sqoopColumnType' },
    {
      header: 'Sqoop Column Type Override',
      accessor: 'sqoopColumnTypeOverride'
    },
    { header: 'Force String', accessor: 'forceString' },
    { header: 'Include In Import', accessor: 'includeInImport' },
    { header: 'Source Primary Key', accessor: 'sourcePrimaryKey' },
    { header: 'Last Update From Source', accessor: 'lastUpdateFromSource' },
    { header: 'Comment', accessor: 'comment' },
    { header: 'Operator Notes', accessor: 'operatorNotes' },
    { header: 'Anonymization Function', accessor: 'anonymizationFunction' },
    { header: 'Edit', isAction: 'edit' }
  ]

  const handleEditClick = (row: Columns) => {
    const rowData: TableSetting[] = [
      { label: 'Column Name', value: row.columnName, type: SettingType.Text }, // Free-text
      {
        label: 'Column Order',
        value: row.columnOrder,
        type: SettingType.Readonly
      }, // Number for order in columns, preliminary readonly
      {
        label: 'Source Column Name',
        value: row.sourceColumnName,
        type: SettingType.Readonly
      }, // Read-only, free-text
      {
        label: 'Column Type',
        value: row.columnType,
        type: SettingType.Readonly
      }, // Read-only, free-text
      {
        label: 'Source Column Type',
        value: row.sourceColumnType,
        type: SettingType.Readonly
      }, // Read-only, free-text
      {
        label: 'Source Database Type',
        value: row.sourceDatabaseType,
        type: SettingType.Readonly
      }, // Read-only, free-text
      {
        label: 'Column Name Override',
        value: row.columnNameOverride,
        type: SettingType.Text
      }, // Free-text
      {
        label: 'Column Type Override',
        value: row.columnTypeOverride,
        type: SettingType.Text
      }, // Free-text
      {
        label: 'Sqoop Column Type',
        value: row.sqoopColumnType,
        type: SettingType.Readonly,
        isConditionsMet: table.importTool === ImportTool.Sqoop
      }, // Read-only, free-text, only active if importTool=sqoop
      {
        label: 'Sqoop Column Type Override',
        value: row.sqoopColumnTypeOverride,
        type: SettingType.Text,
        isConditionsMet: table.importTool === ImportTool.Sqoop
      }, // Free-text, only active if importTool=sqoop
      {
        label: 'Force String',
        value: row.forceString,
        type: SettingType.BooleanOrAuto
      }, // Boolean or Auto (-1)
      {
        label: 'Include In Import',
        value: row.includeInImport,
        type: SettingType.Boolean
      }, // Boolean
      {
        label: 'Source Primary Key',
        value: row.sourcePrimaryKey,
        type: SettingType.Readonly
      }, // Read-only, Boolean
      {
        label: 'Last Update From Source',
        value: row.lastUpdateFromSource,
        type: SettingType.Readonly
      }, // Read-only, Timestamp
      {
        label: 'Comment',
        value: row.comment,
        type: SettingType.Text
      }, // Free-text
      {
        label: 'Operator Notes',
        value: row.operatorNotes,
        type: SettingType.Text
      }, // Free-text
      {
        label: 'Anonymization Function',
        value: row.anonymizationFunction,
        type: SettingType.Enum,
        enumOptions: getEnumOptions('importPhaseType')
      } // Enum mapping for 'Anonymization Function'
    ]

    console.log('handleEditClick row', row)
    setCurrentRow(rowData)
    setModalOpen(true)
  }

  const handleSave = (updatedSettings: TableSetting[]) => {
    // console.log('Updated Row:', updatedSettings)
    const editedTableData = updateTableData(table, updatedSettings, true)
    console.log('editedTableData', editedTableData)
    updateTable(editedTableData, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({ queryKey: ['table', tableParam] })
        console.log('Update successful', response)
        setModalOpen(false)
      },
      onError: (error) => {
        queryClient.invalidateQueries({ queryKey: ['table', tableParam] })
        console.error('Error updating table', error)
      }
    })
  }

  return (
    <div style={{ marginTop: 40 }}>
      <TableList
        columns={columns}
        data={columnsData}
        onEdit={handleEditClick}
      />
      {isModalOpen && currentRow && (
        <EditTableModal
          title="column"
          settings={currentRow}
          onClose={() => setModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default TableColumns
