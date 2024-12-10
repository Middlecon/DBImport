import { useState, useMemo, useCallback } from 'react'
import {
  BulkUpdateImportTables,
  Column,
  DbTable,
  EditSetting,
  HeadersRowInfo,
  ImportSearchFilter,
  UiDbTable,
  UITable
} from '../../utils/interfaces'
import { fetchTableData } from '../../utils/queries'
import './DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkUpdateTable,
  useDeleteImportTable,
  useUpdateTable
} from '../../utils/mutations'
import {
  bulkImportFieldsData,
  importDbTablesEditSettings
} from '../../utils/cardRenderFormatting'
import ConfirmationModal from '../../components/ConfirmationModal'
import TableList from '../../components/TableList'
import ListRowsInfo from '../../components/ListRowsInfo'
import Button from '../../components/Button'
import BulkEditModal from '../../components/BulkEditModal'

function DbTables({
  data,
  queryKeyFilters,
  isLoading,
  headersRowInfo
}: {
  data: UiDbTable[]
  queryKeyFilters: ImportSearchFilter
  isLoading: boolean
  headersRowInfo: HeadersRowInfo
}) {
  const { mutate: bulkUpdateTable } = useBulkUpdateTable()
  const { mutate: updateTable } = useUpdateTable()
  const { mutate: deleteTable } = useDeleteImportTable()
  const queryClient = useQueryClient()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [currentRowsBulk, setCurrentRowsBulk] = useState<UiDbTable[] | []>([])

  const [currentDeleteRow, setCurrentDeleteRow] = useState<UiDbTable>()
  const [tableData, setTableData] = useState<UITable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setIsModalOpen] = useState(false)
  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

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
      { header: 'Actions', isAction: 'delete' }
    ],
    []
  )

  const handleEditClick = async (row: UiDbTable) => {
    console.log('handleEditClick row', row)
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
      setIsModalOpen(true)
    } catch (error) {
      console.error('Failed to fetch table data:', error)
    }
  }

  const handleDeleteIconClick = (row: UiDbTable) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
  }

  const handleBulkEditClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    console.log('handleBulkEditClick selectedRows', selectedRows)
    setCurrentRowsBulk(selectedRows)
    setIsBulkEditModalOpen(true)
  }, [rowSelection, data])

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
          setIsModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  const handleBulkEditSave = (
    bulkChanges: Record<string, string | number | boolean | null>
  ) => {
    const importTablesPks = currentRowsBulk.map((row) => ({
      database: row.database,
      table: row.table
    }))

    const bulkUpdateJson: BulkUpdateImportTables = {
      ...bulkChanges,
      importTables: importTablesPks
    }

    bulkUpdateTable(
      { type: 'import', bulkUpdateJson },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['import', 'search', queryKeyFilters]
          })
          console.log('Update successful', response)
          setIsModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  const currentRowsBulkLength = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => data[index])
  }, [rowSelection, data])

  const currentRowsLength = useMemo(
    () => currentRowsBulkLength.length,
    [currentRowsBulkLength]
  )

  return (
    <div className="db-table-root">
      {data ? (
        <>
          <div className="list-top-info-and-edit">
            <Button
              title={`Edit ${
                currentRowsLength > 0 ? currentRowsLength : ''
              } table${
                currentRowsLength > 1 || currentRowsLength === 0 ? 's' : ''
              }`}
              onClick={handleBulkEditClick}
              disabled={currentRowsLength < 1}
            />
          </div>

          <ListRowsInfo
            filteredData={data}
            headersRowInfo={headersRowInfo}
            itemType="table"
          />
          <TableList
            columns={columns}
            data={data}
            onEdit={handleEditClick}
            onDelete={handleDeleteIconClick}
            isLoading={isLoading}
            rowSelection={rowSelection}
            onRowSelectionChange={setRowSelection}
            enableMultiSelection={true}
          />
        </>
      ) : (
        <div>Loading....</div>
      )}
      {isModalOpen && currentRow && (
        <EditTableModal
          title={`Edit table ${tableName}`}
          settings={currentRow}
          onClose={() => setIsModalOpen(false)}
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
      {isBulkEditModalOpen && (
        <BulkEditModal
          title={`Edit the ${currentRowsLength} selected table${
            currentRowsLength > 1 ? 's' : ''
          }`}
          selectedRows={currentRowsBulk}
          bulkFieldsData={bulkImportFieldsData}
          onSave={handleBulkEditSave}
          onClose={() => setIsBulkEditModalOpen(false)}
          initWidth={584}
        />
      )}
    </div>
  )
}

export default DbTables
