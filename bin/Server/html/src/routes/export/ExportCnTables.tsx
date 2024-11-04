import { useState, useMemo, useCallback } from 'react'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import TableList from '../../components/TableList'
import {
  Column,
  EditSetting,
  ExportCnTablesWithoutEnum,
  UIExportCnTables,
  UIExportTable
} from '../../utils/interfaces'
import { fetchExportTableData, useExportTables } from '../../utils/queries'
import '../import/DbTables.scss'
import { useParams } from 'react-router-dom'
import EditTableModal from '../../components/EditTableModal'
// import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
// import { useUpdateTable } from '../../utils/mutations'
import { useAtom } from 'jotai'
import { exportCnListFiltersAtom } from '../../atoms/atoms'
import { exportCnTablesEditSettings } from '../../utils/cardRenderFormatting'

const checkboxFilters = [
  {
    title: 'Export Type',
    accessor: 'exportType',
    values: ['Full', 'Incremental']
  },
  {
    title: 'Export Tool',
    accessor: 'exportTool',
    values: ['Spark', 'Sqoop']
  }
]

const radioFilters = [
  {
    title: 'Last Update From Hive',
    accessor: 'lastUpdateFromHive  ',
    radioName: 'timestamp',
    badgeContent: ['D', 'W', 'M', 'Y'],
    values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  }
]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     radioName: 'timestamp',
//     badgeContent: ['D', 'W', 'M', 'Y'],
//     values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
//   },
//   {
//     title: 'Include in Airflow',
//     radioName: 'includeInAirflow',
//     badgeContent: ['y', 'n'],
//     values: ['Yes', 'No']
//   }
// ]

function ExportCnTables() {
  const columns: Column<ExportCnTablesWithoutEnum>[] = useMemo(
    () => [
      { header: 'Target Table', accessor: 'targetTable' },
      { header: 'Target Schema', accessor: 'targetSchema' },
      { header: 'Database', accessor: 'database' },
      { header: 'Table', accessor: 'table' },
      { header: 'Export Type', accessor: 'exportType' },
      { header: 'Export Tool', accessor: 'exportTool' },
      {
        header: 'Last Update From Hive',
        accessor: 'lastUpdateFromHive'
      },
      { header: 'Actions', isAction: 'both' }
    ],
    []
  )

  const { connection } = useParams<{ connection: string }>()
  const { data, isLoading } = useExportTables(connection ? connection : null)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [tableData, setTableData] = useState<UIExportTable | null>(null)
  const [tableName, setTableName] = useState<string>('')
  const [isModalOpen, setModalOpen] = useState(false)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const queryClient = useQueryClient()
  // const { mutate: updateTable } = useUpdateTable()

  const [selectedFilters, setSelectedFilters] = useAtom(exportCnListFiltersAtom)

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
      const rowDate = parseTimestamp(row.lastUpdateFromHive)

      return [...checkboxFilters, ...radioFilters].every((filter) => {
        const selectedItems =
          selectedFilters[filter.accessor]?.map((value) => value) || []

        if (selectedItems.length === 0) return true

        // Handling the date filter separately
        if (filter.accessor === 'lastUpdateFromHive  ') {
          const selectedRange = selectedItems[0]
          const startDate = getDateRange(selectedRange)

          if (!rowDate) return false
          return rowDate >= startDate
        }

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters, getDateRange])

  const handleEditClick = async (row: UIExportCnTables) => {
    const { connection, targetSchema, targetTable } = row
    setTableName(targetTable)

    console.log('connection', connection)
    console.log('targetSchema', targetSchema)
    console.log('table', targetTable)

    try {
      const fetchedTableData = await queryClient.fetchQuery({
        queryKey: ['export', 'table', targetTable],
        queryFn: () =>
          fetchExportTableData(connection, targetSchema, targetTable)
      })

      console.log('fetchedTableData', fetchedTableData)

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

    // const editedTableData = updateTableData(tableData, updatedSettings)
    // updateTable(editedTableData, {
    //   onSuccess: (response) => {
    //     queryClient.invalidateQueries({
    //       queryKey: ['tables', tableData.database]
    //     })
    //     console.log('Update successful', response)
    //     setModalOpen(false)
    //   },
    //   onError: (error) => {
    //     console.error('Error updating table', error)
    //   }
    // })
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
          scrollbarMarginTop="50px"
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
