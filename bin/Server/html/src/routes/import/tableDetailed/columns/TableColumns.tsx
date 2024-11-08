import { Column, Columns, EditSetting } from '../../../../utils/interfaces'
import TableList from '../../../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useTable } from '../../../../utils/queries'
import EditTableModal from '../../../../components/EditTableModal'
import { updateTableData } from '../../../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'
import '../../../../components/Loading.scss'
import { importColumnRowDataEdit } from '../../../../utils/cardRenderFormatting'

function TableColumns() {
  const { database, table: tableParam } = useParams<{
    database: string
    table: string
  }>()
  const { data: tableData, isLoading } = useTable(database, tableParam)

  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const [isModalOpen, setModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [rowIndex, setRowIndex] = useState<number>()
  const [dataRefreshTrigger, setDataRefreshTrigger] = useState(0)
  const columnsData = useMemo(
    () => [...(tableData?.columns || [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [tableData, dataRefreshTrigger]
  )

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
      { header: 'Anonymization Function', accessor: 'anonymizationFunction' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: Columns, rowIndex: number | undefined) => {
      if (!tableData) {
        console.error('Table data is not available.')
        return
      }

      const rowData: EditSetting[] = importColumnRowDataEdit(row, tableData)

      setCurrentRow(rowData)
      setRowIndex(rowIndex)
      setModalOpen(true)
    },
    [tableData]
  )

  if (!tableData) return <div className="loading">Loading...</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    const editedTableData = updateTableData(
      tableData,
      updatedSettings,
      true,
      rowIndex
    )

    updateTable(
      { type: 'import', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['import', database, tableParam]
          })
          setDataRefreshTrigger((prev) => prev + 1)
          console.log('Update successful', response)
          setModalOpen(false)
        },
        onError: (error) => {
          queryClient.invalidateQueries({
            queryKey: ['import', database, tableParam]
          })
          console.error('Error updating table', error)
        }
      }
    )
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
