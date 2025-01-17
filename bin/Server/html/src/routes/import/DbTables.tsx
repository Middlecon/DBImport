import { useState, useMemo, useCallback, useEffect } from 'react'
import {
  BulkUpdateImportTables,
  Column,
  DbTable,
  // EditSetting,
  HeadersRowInfo,
  ImportSearchFilter,
  UiDbTable
  // UITable
} from '../../utils/interfaces'
// import { fetchTableData } from '../../utils/queries'
import './DbTables.scss'
// import EditTableModal from '../../components/modals/EditTableModal'
import {
  transformBulkChanges
  // updateTableData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteTables,
  useBulkUpdateTable
  // useDeleteImportTable,
  // useUpdateTable
} from '../../utils/mutations'
import {
  bulkImportFieldsData
  // importDbTablesEditSettings
} from '../../utils/cardRenderFormatting'
import TableList from '../../components/TableList'
import ListRowsInfo from '../../components/ListRowsInfo'
import Button from '../../components/Button'
import { useAtom } from 'jotai'
import { clearRowSelectionAtom } from '../../atoms/atoms'
import BulkEditModal from '../../components/modals/BulkEditModal'
import ConfirmationModal from '../../components/modals/ConfirmationModal'
import RepairTableModal from '../../components/modals/RepairTableModal'
import ResetTableModal from '../../components/modals/ResetTableModal'

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
  const queryClient = useQueryClient()

  const { mutate: bulkUpdateTable } = useBulkUpdateTable()
  const { mutate: bulkDeleteTable } = useBulkDeleteTables()

  // For edit or delete row by Actions icon in table list, may be removed
  // const { mutate: updateTable } = useUpdateTable()
  // const { mutate: deleteTable } = useDeleteImportTable()
  // const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  // const [selectedRow, setSelectedRow] = useState<EditSetting[] | []>([])
  // const [selectedDeleteRow, setSelectedDeleteRow] = useState<UiDbTable>()
  // const [tableData, setTableData] = useState<UITable | null>(null)
  // const [tableName, setTableName] = useState<string>('')
  // const [isEditModalOpen, setIsEditModalOpen] = useState(false)

  const [isRepairTableModalOpen, setIsRepairTableModalOpen] = useState(false)
  const [isResetTableModalOpen, setIsResetTableModalOpen] = useState(false)

  const [selectedRow, setSelectedRow] = useState<UiDbTable>()

  const getPrimaryKeys = (
    row: UiDbTable | undefined
  ): { database: string; table: string } | null => {
    if (!row) return null
    const { database, table } = row
    return { database, table }
  }
  const primaryKeys = useMemo(() => getPrimaryKeys(selectedRow), [selectedRow])

  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)
  const [selectedRowsBulk, setSelectedRowsBulk] = useState<UiDbTable[] | []>([])

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

  const [clearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  useEffect(() => {
    setRowSelection({})
  }, [clearRowSelectionTrigger])

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
      { header: 'Actions', isAction: 'repairAndResetTable' }
    ],
    []
  )

  // For edit or delete row by Actions icon in table list, may be removed
  // const handleEditClick = async (row: UiDbTable) => {
  //   console.log('handleEditClick row', row)
  //   const { database, table } = row
  //   setTableName(table)

  //   try {
  //     const fetchedTableData = await queryClient.fetchQuery({
  //       queryKey: ['import', database, table],
  //       queryFn: () => fetchTableData(database, table)
  //     })

  //     setTableData(fetchedTableData)

  //     const rowData: EditSetting[] = importDbTablesEditSettings(row)

  //     setSelectedRow(rowData)
  //     setIsEditModalOpen(true)
  //   } catch (error) {
  //     console.error('Failed to fetch table data:', error)
  //   }
  // }

  // const handleDeleteIconClick = (row: UiDbTable) => {
  //   setShowDeleteConfirmation(true)
  //   setSelectedDeleteRow(row)
  // }

  // const handleDelete = async (row: UiDbTable) => {
  //   setShowDeleteConfirmation(false)

  //   const { database: databaseDelete, table: tableDelete } = row

  //   deleteTable(
  //     { database: databaseDelete, table: tableDelete },
  //     {
  //       onSuccess: () => {
  //         queryClient.invalidateQueries({
  //           queryKey: ['import'], // Matches all related queries that starts the queryKey with 'import'
  //           exact: false
  //         })
  //         console.log('Delete successful')
  //       },
  //       onError: (error) => {
  //         console.error('Error deleting item', error)
  //       }
  //     }
  //   )
  // }

  // const handleSave = (updatedSettings: EditSetting[]) => {
  //   if (!tableData) {
  //     console.error('Table data is not available.')
  //     return
  //   }

  //   const editedTableData = updateTableData(tableData, updatedSettings)
  //   updateTable(
  //     { type: 'import', table: editedTableData },
  //     {
  //       onSuccess: (response) => {
  //         queryClient.invalidateQueries({
  //           queryKey: ['import', 'search', queryKeyFilters]
  //         })
  //         console.log('Update successful', response)
  //         setIsEditModalOpen(false)
  //       },
  //       onError: (error) => {
  //         console.error('Error updating table', error)
  //       }
  //     }
  //   )
  // }

  const handleRepairIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsRepairTableModalOpen(true)
  }

  const handleResetIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsResetTableModalOpen(true)
  }

  const handleBulkEditClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    console.log('handleBulkEditClick selectedRows', selectedRows)
    setSelectedRowsBulk(selectedRows)
    setIsBulkEditModalOpen(true)
  }, [rowSelection, data])

  const handleBulkEditSave = (
    bulkChanges: Record<string, string | number | boolean | null>
  ) => {
    const importTablesPks = selectedRowsBulk.map((row) => ({
      database: row.database,
      table: row.table
    }))

    // For bulkChanges with enum values
    const transformedChanges = transformBulkChanges('import', bulkChanges)

    console.log('bulkChanges', bulkChanges)
    console.log('transformedChanges', transformedChanges)

    const bulkUpdateJson: BulkUpdateImportTables = {
      ...transformedChanges,
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
          setIsBulkEditModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  const handleBulkDeleteClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    setSelectedRowsBulk(selectedRows)
    setShowBulkDeleteConfirmation(true)
  }, [rowSelection, data])

  const handleBulkDelete = async (rows: UiDbTable[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(({ database, table }) => ({
      database,
      table
    }))

    console.log(bulkDeleteRowsPks)

    bulkDeleteTable(
      { type: 'import', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import'], // Matches all related queries that starts the queryKey with 'import'
            exact: false
          })
          console.log('Delete successful')
          setRowSelection({})
        },
        onError: (error) => {
          console.error('Error deleting item', error)
        }
      }
    )
  }

  const selectedRowsBulkData = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => data[index])
  }, [rowSelection, data])

  const selectedRowsLength = useMemo(
    () => selectedRowsBulkData.length,
    [selectedRowsBulkData]
  )

  return (
    <div className="db-table-root">
      {data ? (
        <>
          <div className="list-top-info-and-edit">
            <Button
              title={`Edit ${
                selectedRowsLength > 0 ? selectedRowsLength : ''
              } table${
                selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
              }`}
              onClick={handleBulkEditClick}
              disabled={selectedRowsLength < 1}
            />
            <Button
              title={`Delete ${
                selectedRowsLength > 0 ? selectedRowsLength : ''
              } table${
                selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
              }`}
              onClick={handleBulkDeleteClick}
              deleteStyle={true}
              disabled={selectedRowsLength < 1}
            />
          </div>

          <ListRowsInfo
            filteredData={data}
            contentTotalRows={headersRowInfo.contentTotalRows}
            contentMaxReturnedRows={headersRowInfo.contentMaxReturnedRows}
            itemType="table"
          />
          <TableList
            columns={columns}
            data={data}
            // For edit or delete row by Actions icon in table list, may be removed
            // onEdit={handleEditClick}
            // onDelete={handleDeleteIconClick}
            onRepair={handleRepairIconClick}
            onReset={handleResetIconClick}
            isLoading={isLoading}
            rowSelection={rowSelection}
            onRowSelectionChange={setRowSelection}
            enableMultiSelection={true}
          />
        </>
      ) : (
        <div>Loading....</div>
      )}
      {/* For edit or delete row by Actions icon in table list, may be removed  */}
      {/* {isEditModalOpen && selectedRow && (
        <EditTableModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit table ${tableName}`}
          settings={selectedRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
      {showDeleteConfirmation && selectedDeleteRow && (
        <ConfirmationModal
          title={`Delete ${selectedDeleteRow.table}`}
          message={`Are you sure that you want to delete table "${selectedDeleteRow.table}"? \nDelete is irreversable.`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleDelete(selectedDeleteRow)}
          onCancel={() => setShowDeleteConfirmation(false)}
          isActive={showDeleteConfirmation}
        />
      )} */}
      {isBulkEditModalOpen && (
        <BulkEditModal
          isBulkEditModalOpen={isBulkEditModalOpen}
          title={`Edit the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }`}
          selectedRows={selectedRowsBulk}
          bulkFieldsData={bulkImportFieldsData}
          onSave={handleBulkEditSave}
          onClose={() => setIsBulkEditModalOpen(false)}
          initWidth={584}
        />
      )}
      {showBulkDeleteConfirmation && selectedRowsBulk && (
        <ConfirmationModal
          title={`Delete the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }`}
          message={`Are you sure that you want to delete the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }? \nDelete is irreversable.`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleBulkDelete(selectedRowsBulk)}
          onCancel={() => setShowBulkDeleteConfirmation(false)}
          isActive={showBulkDeleteConfirmation}
        />
      )}
      {isRepairTableModalOpen && primaryKeys && (
        <RepairTableModal
          type="import"
          primaryKeys={primaryKeys}
          isRepairTableModalOpen={isRepairTableModalOpen}
          onClose={() => setIsRepairTableModalOpen(false)}
        />
      )}
      {isResetTableModalOpen && primaryKeys && (
        <ResetTableModal
          type="import"
          primaryKeys={primaryKeys}
          isResetTableModalOpen={isResetTableModalOpen}
          onClose={() => setIsResetTableModalOpen(false)}
        />
      )}
    </div>
  )
}

export default DbTables
