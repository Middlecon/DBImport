import { useState, useMemo, useCallback, useEffect } from 'react'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import TableList from '../../components/TableList'
import {
  Column,
  DbTable,
  EditSetting,
  UiDbTable,
  UITable
} from '../../utils/interfaces'
import { fetchTableData, useDbTables } from '../../utils/queries'
import './DbTables.scss'
import { useParams } from 'react-router-dom'
import EditTableModal from '../../components/EditTableModal'
import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../utils/mutations'
import { useAtom } from 'jotai'
import { importDbListFiltersAtom } from '../../atoms/atoms'
import { importDbTablesEditSettings } from '../../utils/cardRenderFormatting'

const checkboxFilters = [
  {
    title: 'Import Type',
    accessor: 'importPhaseType',
    values: ['Full', 'Incremental', 'Oracle Flashback', 'MSSQL Change Tracking']
  },
  {
    title: 'ETL Type',
    accessor: 'etlPhaseType',
    values: [
      'Truncate and Insert',
      'Insert only',
      'Merge',
      'Merge with History Audit',
      'Only create external table',
      'None'
    ]
  },
  {
    title: 'Import Tool',
    accessor: 'importTool',
    values: ['Spark', 'Sqoop']
  },
  {
    title: 'ETL Engine',
    accessor: 'etlEngine',
    values: ['Hive', 'Spark']
  }
]

const radioFilters = [
  {
    title: 'Last update from source',
    accessor: 'lastUpdateFromSource',
    radioName: 'timestamp',
    badgeContent: ['D', 'W', 'M', 'Y'],
    values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  },
  {
    title: 'Include In Airflow',
    accessor: 'includeInAirflow',
    radioName: 'includeInAirflow',
    badgeContent: ['t', 'f'],
    values: ['True', 'False']
  }
]

function DbTables() {
  const { database } = useParams<{ database: string }>()
  const { data, isLoading } = useDbTables(database ? database : null)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [tableData, setTableData] = useState<UITable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setModalOpen] = useState(false)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()

  const [selectedFilters, setSelectedFilters] = useAtom(importDbListFiltersAtom)

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

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleSelect = (filterKey: string, items: string[]) => {
    setSelectedFilters((prevFilters) => ({
      ...prevFilters,
      [filterKey]: items
    }))
  }

  const parseTimestamp = (timestamp: string | null): Date | null => {
    if (!timestamp) {
      return null
    }

    try {
      const isoDateString = timestamp.replace(' ', 'T') + 'Z'
      return new Date(isoDateString)
    } catch (error) {
      console.error('Failed to parse timestamp:', error)
      return null
    }
  }

  const getDateRange = useCallback((selection: string) => {
    const now = new Date()
    switch (selection) {
      case 'Last Day':
        return new Date(now.setDate(now.getDate() - 1))
      case 'Last Week':
        return new Date(now.setDate(now.getDate() - 7))
      case 'Last Month':
        return new Date(now.setMonth(now.getMonth() - 1))
      case 'Last Year':
        return new Date(now.setFullYear(now.getFullYear() - 1))
      default:
        return new Date(0)
    }
  }, [])

  const filteredData = useMemo(() => {
    if (!Array.isArray(data)) return []
    return data.filter((row) => {
      const rowDate = parseTimestamp(row.lastUpdateFromSource)

      return [...checkboxFilters, ...radioFilters].every((filter) => {
        const selectedItems =
          selectedFilters[filter.accessor]?.map((value) => value) || []

        if (selectedItems.length === 0) return true

        // Handling the date filter separately
        if (filter.accessor === 'lastUpdateFromSource') {
          const selectedRange = selectedItems[0]
          const startDate = getDateRange(selectedRange)

          if (!rowDate) return false
          return rowDate >= startDate
        }

        if (filter.accessor === 'includeInAirflow') {
          const airflowValue = row[filter.accessor] === true ? 'True' : 'False'
          return selectedItems.includes(airflowValue)
        }

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters, getDateRange])

  const handleEditClick = async (row: UiDbTable) => {
    const { database, table } = row
    setTableName(table)

    try {
      const fetchedTableData = await queryClient.fetchQuery({
        queryKey: ['import', database, table],
        queryFn: () => fetchTableData(database, table)
      })

      console.log('fetchedTableData', fetchedTableData)

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
    console.log('tableData', tableData)

    const editedTableData = updateTableData(tableData, updatedSettings)
    updateTable(
      { type: 'import', table: editedTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['import', tableData.database]
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
      <div className="filters">
        {checkboxFilters.map((filter, index) => (
          <DropdownCheckbox
            key={index}
            items={filter.values}
            title={filter.title}
            selectedItems={selectedFilters[filter.accessor] || []}
            onSelect={(items) => handleSelect(filter.accessor, items)}
            isOpen={openDropdown === filter.accessor}
            onToggle={(isOpen) => handleDropdownToggle(filter.accessor, isOpen)}
          />
        ))}
        {radioFilters.map((filter, index) => (
          <DropdownRadio
            key={index}
            items={filter.values}
            title={filter.title}
            radioName={filter.radioName}
            badgeContent={filter.badgeContent}
            selectedItem={selectedFilters[filter.accessor]?.[0] || null}
            onSelect={(items) => handleSelect(filter.accessor, items)}
            isOpen={openDropdown === filter.accessor}
            onToggle={(isOpen) => handleDropdownToggle(filter.accessor, isOpen)}
          />
        ))}
      </div>

      {filteredData ? (
        <TableList
          columns={columns}
          data={filteredData}
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
