import { useState, useMemo, useCallback } from 'react'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import TableList from '../../components/TableList'
import {
  Column,
  DbTable,
  TableSetting,
  UiDbTable,
  UITable
} from '../../utils/interfaces'
import { fetchTableData, useDbTables } from '../../utils/queries'
import './DbTables.scss'
import {
  getEnumOptions,
  mapDisplayValue,
  reverseMapDisplayValue
} from '../../utils/nameMappings'
import { useOutletContext } from 'react-router-dom'
import EditTableModal from '../../components/EditTableModal'
import { SettingType } from '../../utils/enums'
import { updateTableData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../utils/mutations'

const columns: Column<DbTable>[] = [
  { header: 'Table', accessor: 'table' },
  { header: 'Connection', accessor: 'connection' },
  { header: 'Source Schema', accessor: 'sourceSchema' },
  { header: 'Source Table', accessor: 'sourceTable' },
  { header: 'Import Type', accessor: 'importPhaseType' },
  { header: 'ETL Type', accessor: 'etlPhaseType' },
  { header: 'Import Tool', accessor: 'importTool' },
  { header: 'ETL Engine', accessor: 'etlEngine' },
  { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
  { header: 'Actions', isAction: 'both' }
]

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

interface OutletContextType {
  openDropdown: string | null
  handleDropdownToggle: (dropdownId: string, isOpen: boolean) => void
}

function DbTables() {
  const { data } = useDbTables()
  const [currentRow, setCurrentRow] = useState<TableSetting[] | []>([])
  const [tableData, setTableData] = useState<UITable | null>(null)

  const [isModalOpen, setModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()

  const [selectedFilters, setSelectedFilters] = useState<{
    [key: string]: string[]
  }>({})

  const { openDropdown, handleDropdownToggle } =
    useOutletContext<OutletContextType>()

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
    if (!data) return []

    const mappedFilters = [...checkboxFilters, ...radioFilters].reduce(
      (acc, filter) => {
        const mappedValues = selectedFilters[filter.accessor]?.map((value) =>
          reverseMapDisplayValue(filter.title, value)
        )
        acc[filter.accessor] = mappedValues || []
        return acc
      },
      {} as { [key: string]: string[] }
    )

    return data.filter((row) => {
      const rowDate = parseTimestamp(row.lastUpdateFromSource)

      return Object.keys(mappedFilters).every((filterKey) => {
        const selectedItems = mappedFilters[filterKey]

        if (filterKey === 'lastUpdateFromSource') {
          if (selectedItems.length === 0) return true

          const selectedRange = selectedItems[0]
          const startDate = getDateRange(selectedRange)

          if (!rowDate) return false

          return rowDate >= startDate
        }

        if (selectedItems.length === 0) return true

        const rowValue = row[filterKey as keyof typeof row] as string
        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters, getDateRange])

  const handleEditClick = async (row: UiDbTable) => {
    const { database, table } = row

    try {
      const fetchedTableData = await queryClient.fetchQuery({
        queryKey: ['table', table],
        queryFn: () => fetchTableData(database, table)
      })

      setTableData(fetchedTableData)

      const rowData: TableSetting[] = [
        { label: 'Database', value: row.database, type: SettingType.Readonly }, //Free-text, read-only, default selected db, potentially copyable?
        { label: 'Table', value: row.table, type: SettingType.Readonly }, // Free-text, read-only
        {
          label: '',
          value: '',
          type: SettingType.GroupingSpace
        }, // Layout space
        {
          label: 'Import Type',
          value: mapDisplayValue('importPhaseType', row.importPhaseType),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('importPhaseType')
        }, // Enum mapping for 'Import Type'
        {
          label: 'ETL Type',
          value: mapDisplayValue('etlPhaseType', row.etlPhaseType),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('etlPhaseType')
        }, // Enum mapping for 'ETL Type'
        {
          label: 'Import Tool',
          value: mapDisplayValue('importTool', row.importTool),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('importTool')
        }, // Enum mapping for 'Import Tool'
        {
          label: 'ETL Engine',
          value: mapDisplayValue('etlEngine', row.etlEngine),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('etlEngine')
        } // Enum mapping for 'ETL Engine'
      ]

      setCurrentRow(rowData)
      setModalOpen(true)
    } catch (error) {
      console.error('Failed to fetch table data:', error)
    }
  }

  const handleSave = (updatedSettings: TableSetting[]) => {
    if (!tableData) {
      console.error('Table data is not available.')
      return
    }
    console.log('tableData', tableData)

    const editedTableData = updateTableData(tableData, updatedSettings)
    updateTable(editedTableData, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['tables', tableData.database]
        })
        console.log('Update successful', response)
        setModalOpen(false)
      },
      onError: (error) => {
        console.error('Error updating table', error)
      }
    })
  }

  return (
    <div className="db-table-root">
      <div className="filters">
        {checkboxFilters.map((filter, index) => (
          <DropdownCheckbox
            key={index}
            items={filter.values}
            title={filter.title}
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
        />
      ) : (
        <div>Loading....</div>
      )}
      {isModalOpen && currentRow && (
        <EditTableModal
          title="column"
          settings={currentRow}
          onClose={() => setModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default DbTables
