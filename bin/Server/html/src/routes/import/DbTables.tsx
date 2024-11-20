import { useState, useMemo, useEffect } from 'react'
import TableList from '../../components/TableList'
import {
  Column,
  DbTable,
  EditSetting,
  UiDbTable,
  UITable
} from '../../utils/interfaces'
import { fetchTableData } from '../../utils/queries'
import './DbTables.scss'
import EditTableModal from '../../components/EditTableModal'
import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../utils/mutations'
import { importDbTablesEditSettings } from '../../utils/cardRenderFormatting'
import { useLocation } from 'react-router-dom'

function DbTables({
  data,
  isLoading
}: {
  data: UiDbTable[]
  isLoading: boolean
}) {
  const location = useLocation()
  const query = new URLSearchParams(location.search)
  const database = query.get('database') || null
  const table = query.get('table') || null

  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [tableData, setTableData] = useState<UITable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()

  const [scrollbarMarginTop, setScrollbarMarginTop] = useState('35px')
  const [scrollbarRefresh, setScrollbarRefresh] = useState(0)

  useEffect(() => {
    const viewLayout = document.querySelector(
      '.view-layout-root'
    ) as HTMLElement

    const handleResize = () => {
      if (viewLayout) {
        const newMarginTop =
          viewLayout.offsetWidth <= 1474
            ? '64px'
            : viewLayout.offsetWidth <= 1589
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

  const columns: Column<DbTable>[] = useMemo(
    () => [
      { header: 'Table', accessor: 'table' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Database', accessor: 'database' },
      { header: 'Source Schema', accessor: 'sourceSchema' },
      { header: 'Source Table', accessor: 'sourceTable' },
      { header: 'Import Type', accessor: 'importPhaseType' },
      { header: 'ETL Type', accessor: 'etlPhaseType' },
      { header: 'Import Tool', accessor: 'importTool' },
      { header: 'ETL Engine', accessor: 'etlEngine' },
      { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
      {
        header: 'Include In Airflow',
        accessor: 'includeInAirflow'
      },
      { header: 'Actions', isAction: 'both' }
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [scrollbarRefresh]
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
            queryKey: ['import', 'search', database, table]
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

export default DbTables
