import { useState, useMemo } from 'react'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownSingleSelect from '../../components/DropdownSingleSelect'
import TableList from '../../components/TableList'
import { Column } from '../../utils/interfaces'
import { useDbTables } from '../../utils/queries'
import './DbTables.scss'
import { mapFilterValue } from '../../utils/nameMappings'

const columns: Column[] = [
  { header: 'Table', accessor: 'table' },
  { header: 'Connection', accessor: 'connection' },
  { header: 'Source Schema', accessor: 'sourceSchema' },
  { header: 'Source Table', accessor: 'sourceTable' },
  { header: 'Import Type', accessor: 'importPhaseType' },
  { header: 'ETL Type', accessor: 'etlPhaseType' },
  { header: 'Import Tool', accessor: 'importTool' },
  { header: 'ETL Engine', accessor: 'etlEngine' },
  { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
  { header: 'Actions', isAction: true }
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

function DbTable() {
  const { data } = useDbTables()
  const [selectedFilters, setSelectedFilters] = useState<{
    [key: string]: string[]
  }>({})

  console.log('data Tables', data)

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
      return null // Return null if timestamp is null or undefined
    }

    try {
      const isoDateString = timestamp.replace(' ', 'T') + 'Z'
      return new Date(isoDateString)
    } catch (error) {
      console.error('Failed to parse timestamp:', error)
      return null
    }
  }

  const getDateRange = (selection: string) => {
    const now = new Date()
    let startDate: Date

    switch (selection) {
      case 'Last Day':
        startDate = new Date(now.setDate(now.getDate() - 1))
        break
      case 'Last Week':
        startDate = new Date(now.setDate(now.getDate() - 7))
        break
      case 'Last Month':
        startDate = new Date(now.setMonth(now.getMonth() - 1))
        break
      case 'Last Year':
        startDate = new Date(now.setFullYear(now.getFullYear() - 1))
        break
      default:
        startDate = new Date(0)
    }

    return startDate
  }

  const filteredData = useMemo(() => {
    if (!data) return []

    const mappedFilters = [...checkboxFilters, ...radioFilters].reduce(
      (acc, filter) => {
        const mappedValues = selectedFilters[filter.accessor]?.map((value) =>
          mapFilterValue(filter.title, value)
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
  }, [data, selectedFilters])

  return (
    <div className="db-table-root">
      <div className="scrollable-container">
        <div className="filters">
          {checkboxFilters.map((filter, index) => (
            <DropdownCheckbox
              key={index}
              items={filter.values}
              title={filter.title}
              onSelect={(items) => handleSelect(filter.accessor, items)}
            />
          ))}
          {radioFilters.map((filter, index) => (
            <DropdownSingleSelect
              key={index}
              items={filter.values}
              title={filter.title}
              radioName={filter.radioName}
              badgeContent={filter.badgeContent}
              onSelect={(items) => handleSelect(filter.accessor, items)}
            />
          ))}
        </div>
      </div>

      {filteredData ? (
        <div className="tables-container">
          <div className="scrollable-container">
            <TableList columns={columns} data={filteredData} />
          </div>
        </div>
      ) : (
        <div>Loading....</div>
      )}
    </div>
  )
}

export default DbTable
