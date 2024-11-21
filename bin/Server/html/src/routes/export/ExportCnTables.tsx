import { useState, useMemo, useEffect } from 'react'
import TableList from '../../components/TableList'
import {
  Column,
  EditSetting,
  ExportCnTablesWithoutEnum,
  UIExportCnTables,
  UIExportTable
} from '../../utils/interfaces'
import { fetchExportTableData } from '../../utils/queries'
import '../import/DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import { updateExportTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../utils/mutations'

import { exportCnTablesEditSettings } from '../../utils/cardRenderFormatting'
import { useLocation } from 'react-router-dom'

function ExportCnTables({
  data,
  isLoading
}: {
  data: UIExportCnTables[]
  isLoading: boolean
}) {
  const location = useLocation()
  const query = new URLSearchParams(location.search)
  const connection = query.get('connection') || null
  const targetTable = query.get('targetTable') || null
  const targetSchema = query.get('targetSchema') || null

  const { mutate: updateTable } = useUpdateTable()
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
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
      { header: 'Actions', isAction: 'both' }
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

  const handleSave = (updatedSettings: EditSetting[]) => {
    if (!tableData) {
      console.error('Table data is not available.')
      return
    }
    console.log('tableData', tableData)
    console.log('updatedSettings', updatedSettings)

    const editedTableData = updateExportTableData(tableData, updatedSettings)
    updateTable(
      { type: 'export', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: [
              'export',
              'search',
              connection,
              targetSchema,
              targetTable
            ]
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
    </div>
  )
}

export default ExportCnTables
