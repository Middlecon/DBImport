import { useLocation, useNavigate } from 'react-router-dom'
import { useSearchImportTables } from '../../utils/queries'
import './Import.scss'
import '../../components/Loading.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import CreateImportTableModal from '../../components/CreateImportTableModal'
import {
  EditSetting,
  ImportSearchFilter,
  UiImportSearchFilter
} from '../../utils/interfaces'
import { createTableData } from '../../utils/dataFunctions'
import { useCreateImportTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import ImportSearchFilterTables from '../../components/ImportSearchFilterTables'
import DbTables from './DbTables'
import { useAtom } from 'jotai'
import {
  importTableListFiltersAtom,
  importPersistStateAtom
} from '../../atoms/atoms'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import { reverseMapEnumValue } from '../../utils/nameMappings'

// const checkboxFilters = [
//   {
//     title: 'Import Type',
//     accessor: 'importPhaseType',
//     values: ['Full', 'Incremental', 'Oracle Flashback', 'MSSQL Change Tracking']
//   },
//   {
//     title: 'ETL Type',
//     accessor: 'etlPhaseType',
//     values: [
//       'Truncate and Insert',
//       'Insert only',
//       'Merge',
//       'Merge with History Audit',
//       'Only create external table',
//       'None'
//     ]
//   },
//   {
//     title: 'Import Tool',
//     accessor: 'importTool',
//     values: ['Spark', 'Sqoop']
//   },
//   {
//     title: 'ETL Engine',
//     accessor: 'etlEngine',
//     values: ['Hive', 'Spark']
//   }
// ]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     accessor: 'lastUpdateFromSource',
//     radioName: 'timestamp',
//     badgeContent: ['D', 'W', 'M', 'Y'],
//     values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
//   },
//   {
//     title: 'Include in Airflow',
//     accessor: 'includeInAirflow',
//     radioName: 'includeInAirflow',
//     badgeContent: ['t', 'f'],
//     values: ['True', 'False']
//   }
// ]

const radioFilters = [
  {
    title: 'Last update from source',
    accessor: 'lastUpdateFromSource',
    radioName: 'timestamp',
    badgeContent: ['D', 'W', 'M', 'Y'],
    values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  }
]

function Import() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = [
    'connection',
    'database',
    'table',
    'includeInAirflow',
    'importType',
    'importTool',
    'etlType',
    'etlEngine'
  ]
  const allParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/import', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allParams, navigate])

  const connection = query.get('connection') || null
  const database = query.get('database') || null
  const table = query.get('table') || null
  const includeInAirflow = query.has('includeInAirflow')
    ? query.get('includeInAirflow') === 'True'
      ? true
      : query.get('includeInAirflow') === 'False'
      ? false
      : null
    : null

  const importPhaseType = query.get('importType')
    ? reverseMapEnumValue(
        'import',
        'importType',
        query.get('importType') as string,
        true
      )
    : null
  const importTool = query.get('importTool')
    ? reverseMapEnumValue(
        'import',
        'importTool',
        query.get('importTool') as string,
        true
      )
    : null
  const etlPhaseType = query.get('etlType')
    ? reverseMapEnumValue(
        'import',
        'etlType',
        query.get('etlType') as string,
        true
      )
    : null
  const etlEngine = query.get('etlEngine')
    ? reverseMapEnumValue(
        'import',
        'etlEngine',
        query.get('etlEngine') as string,
        true
      )
    : null

  const filters: ImportSearchFilter = {
    connection,
    database,
    table,
    includeInAirflow,
    importPhaseType,
    importTool,
    etlPhaseType,
    etlEngine
  }

  const queryKey = [
    'import',
    'search',
    connection,
    database,
    table,
    includeInAirflow,
    importPhaseType,
    importTool,
    etlPhaseType,
    etlEngine
  ]

  const { data, isLoading: isSearchLoading } = useSearchImportTables(filters)

  const { mutate: createTable } = useCreateImportTable()
  const queryClient = useQueryClient()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)
  const [, setImportPersistState] = useAtom(importPersistStateAtom)
  const [selectedFilters, setSelectedFilters] = useAtom(
    importTableListFiltersAtom
  )

  const handleShow = (filters: UiImportSearchFilter) => {
    console.log('Import handleShow filters', filters)
    console.log('filters.importPhaseType', filters.importPhaseType)
    console.log('includeInAirflowShow', filters.includeInAirflow)

    const params = new URLSearchParams(location.search)

    if (
      filters.connection &&
      filters.connection !== null &&
      filters.connection.length > 0
    ) {
      params.set('connection', filters.connection)
    } else {
      params.delete('connection')
    }

    if (
      filters.database &&
      filters.database !== null &&
      filters.database.length > 0
    ) {
      params.set('database', filters.database)
    } else {
      params.delete('database')
    }

    if (filters.table && filters.table !== null && filters.table.length > 0) {
      params.set('table', filters.table)
    } else {
      params.delete('table')
    }

    if (filters.includeInAirflow !== null) {
      params.set('includeInAirflow', filters.includeInAirflow)
    } else {
      params.delete('includeInAirflow')
    }

    if (filters.importPhaseType !== null) {
      params.set('importType', filters.importPhaseType)
    } else {
      params.delete('importType')
    }

    if (filters.importTool !== null) {
      params.set('importTool', filters.importTool)
    } else {
      params.delete('importTool')
    }

    if (filters.etlPhaseType !== null) {
      params.set('etlType', filters.etlPhaseType)
    } else {
      params.delete('etlType')
    }

    if (filters.etlEngine !== null) {
      params.set('etlEngine', filters.etlEngine)
    } else {
      params.delete('etlEngine')
    }

    const connectionUrlString =
      filters.connection !== null && filters.connection.length > 0
        ? `connection=${params.get('connection') || null}`
        : null
    const databaseUrlString =
      filters.database !== null && filters.database.length > 0
        ? `database=${params.get('database') || null}`
        : null
    const tableUrlString =
      filters.table !== null && filters.table.length > 0
        ? `table=${params.get('table') || null}`
        : null
    const includeInAirflowUrlString =
      filters.includeInAirflow !== null
        ? `includeInAirflow=${params.get('includeInAirflow') || null}`
        : null
    const importTypeUrlString =
      filters.importPhaseType !== null
        ? `importType=${params.get('importType') || null}`
        : null
    const importToolUrlString =
      filters.importTool !== null
        ? `importTool=${params.get('importTool') || null}`
        : null
    const etlTypeUrlString =
      filters.etlPhaseType !== null
        ? `etlType=${params.get('etlType') || null}`
        : null
    const etlEngineUrlString =
      filters.etlEngine !== null
        ? `etlEngine=${params.get('etlEngine') || null}`
        : null

    const orderedSearch = [
      connectionUrlString,
      databaseUrlString,
      tableUrlString,
      includeInAirflowUrlString,
      importTypeUrlString,
      importToolUrlString,
      etlTypeUrlString,
      etlEngineUrlString
    ]
      .filter((param) => param !== null) // Remove null values
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setImportPersistState(`/import?${orderedSearch}`)

      navigate(`/import?${orderedSearch}`, { replace: true })
    }
  }

  const mostCommonConnection = useMemo(() => {
    if (!data || !data.tables || data.tables.length === 0) return null
    const connectionCounts = data.tables.reduce((acc, row) => {
      const connection = row.connection
      if (connection) {
        acc[connection] = (acc[connection] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    return Object.keys(connectionCounts).reduce((a, b) =>
      connectionCounts[a] > connectionCounts[b] ? a : b
    )
  }, [data])

  const mostCommonDatabase = useMemo(() => {
    if (!data || !data.tables || data.tables.length === 0) return null
    const databaseCounts = data.tables.reduce((acc, row) => {
      const database = row.database
      if (database) {
        acc[database] = (acc[database] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    return Object.keys(databaseCounts).reduce((a, b) =>
      databaseCounts[a] > databaseCounts[b] ? a : b
    )
  }, [data])

  const handleSave = (newTableData: EditSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: [
            'import',
            'search',
            connection,
            database,
            table,
            includeInAirflow,
            importPhaseType,
            importTool,
            etlPhaseType,
            etlEngine
          ]
        })
        console.log('Create successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error creating table', error)
      }
    })
  }

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
    if (!data || !Array.isArray(data.tables)) return []
    return data.tables.filter((row) => {
      const rowDate = parseTimestamp(row.lastUpdateFromSource)

      // return [...checkboxFilters, ...radioFilters].every((filter) => {
      return [...radioFilters].every((filter) => {
        const selectedItems =
          selectedFilters[filter.accessor]?.map((value) => value) || []

        if (selectedItems.length === 0) return true

        if (filter.accessor === 'lastUpdateFromSource') {
          const selectedRange = selectedItems[0]
          const startDate = getDateRange(selectedRange)

          if (!rowDate) return false
          return rowDate >= startDate
        }

        // if (filter.accessor === 'includeInAirflow') {
        //   const airflowValue = row[filter.accessor] === true ? 'True' : 'False'
        //   return selectedItems.includes(airflowValue)
        // }

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters, getDateRange])

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Import</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create table"
              onClick={() => setCreateModalOpen(true)}
              fontFamily={`'Work Sans Variable', sans-serif`}
              fontSize="14px"
              padding="4px 13px 7.5px 9px"
            />
            <ImportSearchFilterTables
              isSearchFilterOpen={openDropdown === 'searchFilter'}
              onToggle={(isSearchFilterOpen: boolean) =>
                handleDropdownToggle('searchFilter', isSearchFilterOpen)
              }
              onShow={handleShow}
            />
          </div>
        </div>
        <div className="filters">
          {/* {checkboxFilters.map((filter, index) => (
            <DropdownCheckbox
              key={index}
              items={filter.values}
              title={filter.title}
              selectedItems={selectedFilters[filter.accessor] || []}
              onSelect={(items) => handleSelect(filter.accessor, items)}
              isOpen={openDropdown === filter.accessor}
              onToggle={(isOpen) =>
                handleDropdownToggle(filter.accessor, isOpen)
              }
            />
          ))} */}
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
              onToggle={(isOpen) =>
                handleDropdownToggle(filter.accessor, isOpen)
              }
            />
          ))}
        </div>

        {isCreateModalOpen && (
          <CreateImportTableModal
            database={mostCommonDatabase ? mostCommonDatabase : null}
            prefilledConnection={
              mostCommonConnection ? mostCommonConnection : null
            }
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}

        {data && Array.isArray(data.tables) ? (
          <>
            <p className="list-rows-info">
              {`Showing ${filteredData.length} (of ${data.headersRowInfo.contentRows}) rows`}
            </p>
            <DbTables
              data={filteredData}
              isLoading={isSearchLoading}
              queryKey={queryKey}
            />
          </>
        ) : isSearchLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="import-text-block">
            <p>
              Get and show export tables by the filter search above to the
              right.
            </p>
          </div>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Import
