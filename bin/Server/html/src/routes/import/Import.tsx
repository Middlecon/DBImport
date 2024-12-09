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

  const filters: ImportSearchFilter = useMemo(
    () => ({
      connection,
      database,
      table,
      includeInAirflow,
      importPhaseType,
      importTool,
      etlPhaseType,
      etlEngine
    }),
    [
      connection,
      database,
      table,
      includeInAirflow,
      importPhaseType,
      importTool,
      etlPhaseType,
      etlEngine
    ]
  )

  const { data, isLoading: isSearchLoading } = useSearchImportTables(filters)

  const { mutate: createTable } = useCreateImportTable()
  const queryClient = useQueryClient()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)
  const [, setImportPersistState] = useAtom(importPersistStateAtom)
  const [selectedFilters, setSelectedFilters] = useAtom(
    importTableListFiltersAtom
  )

  const handleShow = (uiFilters: UiImportSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: [keyof UiImportSearchFilter, string][] = [
      ['connection', 'connection'],
      ['database', 'database'],
      ['table', 'table'],
      ['includeInAirflow', 'includeInAirflow'],
      ['importPhaseType', 'importType'],
      ['importTool', 'importTool'],
      ['etlPhaseType', 'etlType'],
      ['etlEngine', 'etlEngine']
    ]

    filterKeys.forEach(([key, paramName]) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(paramName, String(value))
      } else {
        params.delete(paramName)
      }
    })

    const orderedSearch = filterKeys
      .map(([, paramName]) =>
        params.has(paramName)
          ? `${paramName}=${params.get(paramName) || ''}`
          : null
      )
      .filter((param) => param !== null)
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

    const keys = Object.keys(connectionCounts)
    if (keys.length === 0) return null

    return keys.reduce((a, b) =>
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

    const keys = Object.keys(databaseCounts)
    if (keys.length === 0) return null

    return keys.reduce((a, b) =>
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
          queryKey: ['import', 'search', filters]
        })
        queryClient.invalidateQueries({
          queryKey: ['databases']
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
            <DbTables
              data={filteredData}
              headersRowInfo={data.headersRowInfo}
              queryKeyFilters={filters}
              isLoading={isSearchLoading}
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
