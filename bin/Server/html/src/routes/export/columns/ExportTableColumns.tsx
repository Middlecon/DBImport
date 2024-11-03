import { useCallback, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'
import '../../../components/Loading.scss'
import { Column, EditSetting, ExportColumns } from '../../../utils/interfaces'
import { useUpdateTable } from '../../../utils/mutations'
import { useExportTable } from '../../../utils/queries'
import TableList from '../../../components/TableList'
import EditTableModal from '../../../components/EditTableModal'
import { updateExportTableData } from '../../../utils/dataFunctions'
import { exportColumnRowDataEdit } from '../../../utils/cardRenderFormatting'

function ExportTableColumns() {
  const {
    connection,
    schema,
    table: tableParam
  } = useParams<{
    connection: string
    schema: string
    table: string
  }>()
  const { data: tableData, isLoading } = useExportTable(
    connection,
    schema,
    tableParam
  )

  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const [isModalOpen, setModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [dataRefreshTrigger, setDataRefreshTrigger] = useState(0)
  const columnsData = useMemo(
    () => [...(tableData?.columns || [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [tableData, dataRefreshTrigger]
  )

  const columns: Column<ExportColumns>[] = useMemo(
    () => [
      { header: 'Column Name', accessor: 'columnName' },
      { header: 'Column Order', accessor: 'columnOrder' },
      { header: 'Target Column Name', accessor: 'targetColumnName' },
      { header: 'Column Type', accessor: 'columnType' },
      { header: 'Target Column Type', accessor: 'targetColumnType' },
      { header: 'Last Update From Hive', accessor: 'lastUpdateFromHive' },
      { header: 'Include In Export', accessor: 'includeInExport' },
      { header: 'Comment', accessor: 'comment' },
      { header: 'Operator Notes', accessor: 'operatorNotes' },
      { header: 'Edit', isAction: 'edit' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: ExportColumns) => {
      if (!tableData) {
        console.error('Table data is not available.')
        return
      }

      const rowData: EditSetting[] = exportColumnRowDataEdit(row)

      setCurrentRow(rowData)
      setModalOpen(true)
    },
    [tableData]
  )

  if (!tableData) return <div className="loading">No data found yet.</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    console.log('updatedSettings export', updatedSettings)
    const editedTableData = updateExportTableData(
      tableData,
      updatedSettings,
      true
    )
    updateTable(
      { type: 'export', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['export', connection, tableParam]
          })
          setDataRefreshTrigger((prev) => prev + 1)
          console.log('Update successful', response)
          setModalOpen(false)
        },
        onError: (error) => {
          queryClient.invalidateQueries({
            queryKey: ['export', connection, tableParam]
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

export default ExportTableColumns
