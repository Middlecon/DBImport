import { useState, useMemo, useCallback } from 'react'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownSingleSelect from '../../components/DropdownSingleSelect'
import TableList from '../../components/TableList'
import { Column, DbTable } from '../../utils/interfaces'
import { useDbTables } from '../../utils/queries'
import './DbTables.scss'
import { reverseMapDisplayValue } from '../../utils/nameMappings'
import { useOutletContext } from 'react-router-dom'

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

const singleSelectFilters = [
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
  const [selectedFilters, setSelectedFilters] = useState<{
    [key: string]: string[]
  }>({})
  const { openDropdown, handleDropdownToggle } =
    useOutletContext<OutletContextType>()

  // console.log('data Tables', data)

  // if (isLoading) {
  //   return <p>Loading...</p>
  // }

  // if (isError) {
  //   return <p>Error: {error?.message}</p>
  // }

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

    const mappedFilters = [...checkboxFilters, ...singleSelectFilters].reduce(
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
        {singleSelectFilters.map((filter, index) => (
          <DropdownSingleSelect
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
        <TableList columns={columns} data={filteredData} />
      ) : (
        <div>Loading....</div>
      )}
    </div>
  )
}

export default DbTables
