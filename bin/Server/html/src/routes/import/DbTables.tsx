import { useState, useMemo } from 'react'
import {
  Column,
  DbTable,
  EditSetting,
  ImportSearchFilter,
  UiDbTable,
  UITable
} from '../../utils/interfaces'
import { fetchTableData } from '../../utils/queries'
import './DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useDeleteImportTable, useUpdateTable } from '../../utils/mutations'
import { importDbTablesEditSettings } from '../../utils/cardRenderFormatting'
import ConfirmationModal from '../../components/ConfirmationModal'
import TableList from '../../components/TableList'

function DbTables({
  data,
  queryKeyFilters,
  isLoading
}: {
  data: UiDbTable[]
  queryKeyFilters: ImportSearchFilter
  isLoading: boolean
}) {
  const { mutate: updateTable } = useUpdateTable()
  const { mutate: deleteTable } = useDeleteImportTable()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [currentDeleteRow, setCurrentDeleteRow] = useState<UiDbTable>()
  const [tableData, setTableData] = useState<UITable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setModalOpen] = useState(false)
  const queryClient = useQueryClient()

  const columns: Column<DbTable>[] = useMemo(
    () => [
      { header: 'Table', accessor: 'table' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Database', accessor: 'database' },
      { header: 'Source Schema', accessor: 'sourceSchema' },
      { header: 'Source Table', accessor: 'sourceTable' },
      { header: 'Import Type', accessor: 'importPhaseType' },
      { header: 'Import Tool', accessor: 'importTool' },
      { header: 'ETL Type', accessor: 'etlPhaseType' },
      { header: 'ETL Engine', accessor: 'etlEngine' },
      { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
      {
        header: 'Include in Airflow',
        accessor: 'includeInAirflow'
      },
      { header: 'Actions', isAction: 'editAndDelete' }
    ],
    []
  )

  const handleEditClick = async (row: UiDbTable) => {
    const { database, table } = row
    setTableName(table)

    try {
      const fetchedTableData = await queryClient.fetchQuery({
        queryKey: ['import', database, table],
        queryFn: () => fetchTableData(database, table)
      })

      setTableData(fetchedTableData)

      const rowData: EditSetting[] = importDbTablesEditSettings(row)

      setCurrentRow(rowData)
      setModalOpen(true)
    } catch (error) {
      console.error('Failed to fetch table data:', error)
    }
  }

  const handleDeleteIconClick = (row: UiDbTable) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
  }

  const handleDelete = async (row: UiDbTable) => {
    setShowDeleteConfirmation(false)

    const { database: databaseDelete, table: tableDelete } = row

    deleteTable(
      { database: databaseDelete, table: tableDelete },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import'], // Matches all related queries that starts the queryKey with 'import'
            exact: false
          })
          console.log('Delete successful')
        },
        onError: (error) => {
          console.error('Error deleting item', error)
        }
      }
    )
  }

  const handleSave = (updatedSettings: EditSetting[]) => {
    if (!tableData) {
      console.error('Table data is not available.')
      return
    }

    const editedTableData = updateTableData(tableData, updatedSettings)
    updateTable(
      { type: 'import', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['import', 'search', queryKeyFilters]
          })
          console.log('Update successful', response)
          setModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  return (
    <div className="db-table-root">
      {data ? (
        <TableList
          columns={columns}
          data={data}
          onEdit={handleEditClick}
          onDelete={handleDeleteIconClick}
          isLoading={isLoading}
        />
      ) : (
        <div>Loading....</div>
      )}
      {isModalOpen && currentRow && (
        <EditTableModal
          title={`Edit table ${tableName}`}
          settings={currentRow}
          onClose={() => setModalOpen(false)}
          onSave={handleSave}
        />
      )}
      {showDeleteConfirmation && currentDeleteRow && (
        <ConfirmationModal
          title={`Delete ${currentDeleteRow.table}`}
          message={`Are you sure that you want to delete table "${currentDeleteRow.table}"? \nDelete is irreversable.`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleDelete(currentDeleteRow)}
          onCancel={() => setShowDeleteConfirmation(false)}
          isActive={showDeleteConfirmation}
        />
      )}
    </div>
  )
}

export default DbTables
