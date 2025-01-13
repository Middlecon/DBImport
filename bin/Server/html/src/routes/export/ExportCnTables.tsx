import { useState, useMemo, useCallback, useEffect } from 'react'
import TableList from '../../components/TableList'
import {
  BulkUpdateExportTables,
  Column,
  EditSetting,
  ExportCnTablesWithoutEnum,
  ExportSearchFilter,
  HeadersRowInfo,
  UIExportCnTables,
  UIExportTable
} from '../../utils/interfaces'
import { fetchExportTableData } from '../../utils/queries'
import '../import/DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import {
  transformBulkChanges,
  updateExportTableData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteTables,
  useBulkUpdateTable,
  useDeleteExportTable,
  useUpdateTable
} from '../../utils/mutations'

import {
  bulkExportFieldsData,
  exportCnTablesEditSettings
} from '../../utils/cardRenderFormatting'
import ConfirmationModal from '../../components/ConfirmationModal'
import Button from '../../components/Button'
import BulkEditModal from '../../components/BulkEditModal'
import ListRowsInfo from '../../components/ListRowsInfo'
import { useAtom } from 'jotai'
import { clearRowSelectionAtom } from '../../atoms/atoms'

function ExportCnTables({
  data,
  queryKeyFilters,
  isLoading,
  headersRowInfo
}: {
  data: UIExportCnTables[]
  queryKeyFilters: ExportSearchFilter
  isLoading: boolean
  headersRowInfo: HeadersRowInfo
}) {
  const queryClient = useQueryClient()

  const { mutate: updateTable } = useUpdateTable()
  const { mutate: deleteTable } = useDeleteExportTable()

  const { mutate: bulkUpdateTable } = useBulkUpdateTable()
  const { mutate: bulkDeleteTable } = useBulkDeleteTables()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [selectedRow, setSelectedRow] = useState<EditSetting[] | []>([])

  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)
  const [selectedRowsBulk, setSelectedRowsBulk] = useState<
    UIExportCnTables[] | []
  >([])

  const [selectedDeleteRow, setSelectedDeleteRow] = useState<UIExportCnTables>()
  const [tableData, setTableData] = useState<UIExportTable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

  const [clearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  useEffect(() => {
    setRowSelection({})
  }, [clearRowSelectionTrigger])

  const columns: Column<ExportCnTablesWithoutEnum>[] = useMemo(
    () => [
      { header: 'Target Table', accessor: 'targetTable' },
      { header: 'Target Schema', accessor: 'targetSchema' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Database', accessor: 'database' },
      { header: 'Table', accessor: 'table' },
      { header: 'Export Type', accessor: 'exportType' },
      { header: 'Export Tool', accessor: 'exportTool' },
      {
        header: 'Last update from Hive',
        accessor: 'lastUpdateFromHive'
      },
      {
        header: 'Include in Airflow',
        accessor: 'includeInAirflow'
      }
      // { header: 'Actions', isAction: 'delete' }
    ],
    []
  )

  const handleEditClick = async (row: UIExportCnTables) => {
    const { connection, targetSchema, targetTable } = row
    setTableName(targetTable)

    try {
      const fetchedTableData = await queryClient.fetchQuery({
        queryKey: ['export', connection, targetTable],
        queryFn: () =>
          fetchExportTableData(connection, targetSchema, targetTable)
      })

      setTableData(fetchedTableData)

      const rowData: EditSetting[] = exportCnTablesEditSettings(row)

      setSelectedRow(rowData)
      setIsEditModalOpen(true)
    } catch (error) {
      console.error('Failed to fetch table data:', error)
    }
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

  const handleDeleteIconClick = (row: UIExportCnTables) => {
    setShowDeleteConfirmation(true)
    setSelectedDeleteRow(row)
  }

  const handleDelete = async (row: UIExportCnTables) => {
    setShowDeleteConfirmation(false)

    const {
      connection: connectionDelete,
      targetSchema: targetSchemaDelete,
      targetTable: targetTableDelete
    } = row

    deleteTable(
      {
        connection: connectionDelete,
        targetSchema: targetSchemaDelete,
        targetTable: targetTableDelete
      },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export'], // Matches all related queries that starts the queryKey with 'export'
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

    const editedTableData = updateExportTableData(tableData, updatedSettings)
    updateTable(
      { type: 'export', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['export', 'search', queryKeyFilters]
          })
          console.log('Update successful', response)
          setIsEditModalOpen(false)
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
    const exportTablesPks = selectedRowsBulk.map((row) => ({
      connection: row.connection,
      targetSchema: row.targetSchema,
      targetTable: row.targetTable
    }))

    // For bulkChanges with enum values
    const transformedChanges = transformBulkChanges('export', bulkChanges)

    console.log('bulkChanges', bulkChanges)
    console.log('transformedChanges', transformedChanges)

    const bulkUpdateJson: BulkUpdateExportTables = {
      ...transformedChanges,
      exportTables: exportTablesPks
    }

    console.log(bulkUpdateJson)

    bulkUpdateTable(
      { type: 'export', bulkUpdateJson },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['export', 'search', queryKeyFilters]
          })
          console.log('Update successful', response)
          setIsEditModalOpen(false)
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

  const handleBulkDelete = async (rows: UIExportCnTables[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(
      ({ connection, targetSchema, targetTable }) => ({
        connection,
        targetSchema,
        targetTable
      })
    )

    console.log(bulkDeleteRowsPks)

    bulkDeleteTable(
      { type: 'export', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export'], // Matches all related queries that starts the queryKey with 'export'
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
          <div
            className="list-top-info-and-edit"
            // style={{ visibility: 'hidden' }}
          >
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
            onEdit={handleEditClick}
            onDelete={handleDeleteIconClick}
            isLoading={isLoading}
            noLinkOnTableName={true}
            rowSelection={rowSelection}
            onRowSelectionChange={setRowSelection}
            enableMultiSelection={true}
          />
        </>
      ) : (
        <div>Loading....</div>
      )}
      {isEditModalOpen && selectedRow && (
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
          title={`Delete ${selectedDeleteRow.targetTable}`}
          message={`Are you sure that you want to delete \n\n target table "${selectedDeleteRow.targetTable}"? Delete is irreversable.`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleDelete(selectedDeleteRow)}
          onCancel={() => setShowDeleteConfirmation(false)}
          isActive={showDeleteConfirmation}
        />
      )}
      {isBulkEditModalOpen && (
        <BulkEditModal
          isBulkEditModalOpen={isBulkEditModalOpen}
          title={`Edit the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }`}
          selectedRows={selectedRowsBulk}
          bulkFieldsData={bulkExportFieldsData}
          // onBulkChange={handleBulkChange}
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
          isActive={showDeleteConfirmation}
        />
      )}
    </div>
  )
}

export default ExportCnTables
