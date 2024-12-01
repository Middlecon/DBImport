import { useState, useMemo, useEffect } from 'react'
import TableList from '../../components/TableList'
import {
  Column,
  EditSetting,
  ExportCnTablesWithoutEnum,
  ExportSearchFilter,
  UIExportCnTables,
  UIExportTable
} from '../../utils/interfaces'
import { fetchExportTableData } from '../../utils/queries'
import '../import/DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import { updateExportTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useDeleteExportTable, useUpdateTable } from '../../utils/mutations'

import { exportCnTablesEditSettings } from '../../utils/cardRenderFormatting'
import ConfirmationModal from '../../components/ConfirmationModal'

function ExportCnTables({
  data,
  queryKeyFilters,
  isLoading
}: {
  data: UIExportCnTables[]
  queryKeyFilters: ExportSearchFilter
  isLoading: boolean
}) {
  const { mutate: updateTable } = useUpdateTable()
  const { mutate: deleteTable } = useDeleteExportTable()
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [currentDeleteRow, setCurrentDeleteRow] = useState<UIExportCnTables>()
  const [tableData, setTableData] = useState<UIExportTable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setModalOpen] = useState(false)
  const queryClient = useQueryClient()

  const [scrollbarMarginTop, setScrollbarMarginTop] = useState('35px')
  const [scrollbarRefresh, setScrollbarRefresh] = useState(0)

  useEffect(() => {
    const viewLayout = document.querySelector(
      '.view-layout-root'
    ) as HTMLElement

    const handleResize = () => {
      if (viewLayout) {
        const newMarginTop =
          viewLayout.offsetWidth <= 1173
            ? '65px'
            : viewLayout.offsetWidth <= 1307
            ? '50px'
            : '35px'
        if (newMarginTop !== scrollbarMarginTop) {
          setScrollbarMarginTop(newMarginTop)
          setScrollbarRefresh((prev) => prev + 1) // Triggers refresh
        }
      }
    }

    const resizeObserver = new ResizeObserver(handleResize)
    if (viewLayout) resizeObserver.observe(viewLayout)

    return () => {
      if (viewLayout) resizeObserver.unobserve(viewLayout)
    }
  }, [scrollbarMarginTop])

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
      },
      { header: 'Actions', isAction: 'editAndDelete' }
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [scrollbarRefresh]
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

      setCurrentRow(rowData)
      setModalOpen(true)
    } catch (error) {
      console.error('Failed to fetch table data:', error)
    }
  }

  const handleDeleteIconClick = (row: UIExportCnTables) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
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
          scrollbarMarginTop={scrollbarMarginTop}
          isExport={true}
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
          title={`Delete ${currentDeleteRow.targetTable}`}
          message={`Are you sure that you want to delete \n\n target table "${currentDeleteRow.targetTable}"? Delete is irreversable.`}
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

export default ExportCnTables
