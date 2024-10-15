import { Column, Columns, TableSetting } from '../../../../utils/interfaces'
import TableList from '../../../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useTable } from '../../../../utils/queries'
import EditTableModal from '../../../../components/EditTableModal'
import { ImportTool, SettingType } from '../../../../utils/enums'
import { updateTableData } from '../../../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'
import { getEnumOptions } from '../../../../utils/nameMappings'

function TableColumns() {
  const { database, table: tableParam } = useParams<{
    database: string
    table: string
  }>()
  const { data: table, isFetching, isLoading } = useTable(database, tableParam)

  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const [isModalOpen, setModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<TableSetting[] | []>([])

  const columns: Column<Columns>[] = useMemo(
    () => [
      { header: 'Column Name', accessor: 'columnName' },
      { header: 'Column Order', accessor: 'columnOrder' },
      { header: 'Source Column Name', accessor: 'sourceColumnName' },
      { header: 'Column Type', accessor: 'columnType' },
      { header: 'Source Column Type', accessor: 'sourceColumnType' },
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
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: Columns) => {
      if (!table) {
        console.error('Table data is not available.')
        return
      }

      const rowData: TableSetting[] = [
        {
          label: 'Column Name',
          value: row.columnName,
          type: SettingType.Readonly
        }, // Read-only, , free-text
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
          type: SettingType.BooleanOrDefaultFromConfig
        }, // Boolean or Auto (-1)
        {
          label: 'Include In Import',
          value: row.includeInImport,
          type: SettingType.BooleanNumber
        }, // Boolean from number 1 or 0
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
          enumOptions: getEnumOptions('anonymizationFunction')
        } // Enum mapping for 'Anonymization Function'
      ]

      setCurrentRow(rowData)
      setModalOpen(true)
    },
    [table]
  )

  if (isFetching) return <div>Loading...</div>
  if (!table) return <div>No data found.</div>

  const columnsData = table.columns

  const handleSave = (updatedSettings: TableSetting[]) => {
    const editedTableData = updateTableData(table, updatedSettings, true)
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
      {columnsData.length > 0 ? (
        <TableList
          columns={columns}
          data={columnsData}
          onEdit={handleEditClick}
          isLoading={isLoading}
          scrollbarMarginTop="78px"
        />
      ) : (
        <p
          style={{
            padding: ' 40px 50px 44px 50px',
            backgroundColor: 'white',
            borderRadius: 7,
            textAlign: 'center'
          }}
        >
          No columns yet in this table.
        </p>
      )}

      {isModalOpen && currentRow && (
        <EditTableModal
          title={`Edit column ${currentRow[0].value}`}
          settings={currentRow}
          onClose={() => setModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default TableColumns
