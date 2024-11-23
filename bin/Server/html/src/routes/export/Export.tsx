import { useLocation, useNavigate } from 'react-router-dom'
import { useSearchExportTables } from '../../utils/queries'
import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import {
  EditSetting,
  ExportSearchFilter,
  // ExportSearchFilter,
  UiExportSearchFilter
} from '../../utils/interfaces'
import { useAtom } from 'jotai'
import {
  exportCnListFiltersAtom,
  exportPersistStateAtom
} from '../../atoms/atoms'
import CreateExportTableModal from '../../components/CreateExportTableModal'
import { createExportTableData } from '../../utils/dataFunctions'
import { useCreateExportTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import ExportCnTables from './ExportCnTables'
import ExportSearchFilterTables from '../../components/ExportSearchFilterTables'
import { reverseMapEnumValue } from '../../utils/nameMappings'

// const checkboxFilters = [
//   {
//     title: 'Export Type',
//     accessor: 'exportType',
//     values: ['Full', 'Incremental']
//   },
//   {
//     title: 'Export Tool',
//     accessor: 'exportTool',
//     values: ['Spark', 'Sqoop']
//   }
// ]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     accessor: 'lastUpdateFromHive  ',
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
    accessor: 'lastUpdateFromHive  ',
    radioName: 'timestamp',
    badgeContent: ['D', 'W', 'M', 'Y'],
    values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  }
]

function Export() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = [
    'connection',
    'targetTable',
    'targetSchema',
    'includeInAirflow',
    'exportType',
    'exportTool'
  ]
  const allParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/export', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allParams, navigate])

  const connection = query.get('connection') || null
  const targetTable = query.get('targetTable') || null
  const targetSchema = query.get('targetSchema') || null
  const includeInAirflow = query.has('includeInAirflow')
    ? query.get('includeInAirflow') === 'True'
      ? true
      : query.get('includeInAirflow') === 'False'
      ? false
      : null
    : null

  const exportType = query.get('exportType')
    ? reverseMapEnumValue(
        'export',
        'exportType',
        query.get('exportType') as string,
        true
      )
    : null
  const exportTool = query.get('exportTool')
    ? reverseMapEnumValue(
        'export',
        'exportTool',
        query.get('exportTool') as string,
        true
      )
    : null

  const filters: ExportSearchFilter = useMemo(
    () => ({
      connection,
      targetTable,
      targetSchema,
      includeInAirflow,
      exportType,
      exportTool
    }),
    [
      connection,
      targetTable,
      targetSchema,
      includeInAirflow,
      exportType,
      exportTool
    ]
  )

  const { data, isLoading: isSearchLoading } = useSearchExportTables(filters)

  const { mutate: createTable } = useCreateExportTable()
  const queryClient = useQueryClient()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [, setExportPersistState] = useAtom(exportPersistStateAtom)
  const [selectedFilters, setSelectedFilters] = useAtom(exportCnListFiltersAtom)

  // const handleSelect = (item: string | null) => {
  //   setSelectedFilters({})
  //   navigate(`/export/${item}`)
  // }

  const handleShow = (uiFilters: UiExportSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiExportSearchFilter)[] = [
      'connection',
      'targetTable',
      'targetSchema',
      'includeInAirflow',
      'exportType',
      'exportTool'
    ]

    filterKeys.forEach((key) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(key, String(value))
      } else {
        params.delete(key)
      }
    })

    const orderedSearch = filterKeys
      .map((key) =>
        params.has(key) ? `${key}=${params.get(key) || ''}` : null
      )
      .filter((param) => param !== null)
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setExportPersistState(`/export?${orderedSearch}`)
      navigate(`/export?${orderedSearch}`, { replace: true })
    }
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

  const handleSave = (newTableData: EditSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createExportTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['export', 'search', filters]
        })
        console.log('Create successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error creating table', error)
      }
    })
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
      const rowDate = parseTimestamp(row.lastUpdateFromHive)

      // return [...checkboxFilters, ...radioFilters].every((filter) => {
      return [...radioFilters].every((filter) => {
        const selectedItems =
          selectedFilters[filter.accessor]?.map((value) => value) || []

        if (selectedItems.length === 0) return true

        // Handling the date filter separately
        if (filter.accessor === 'lastUpdateFromHive') {
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
          <h1>Export</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create table"
              onClick={() => setCreateModalOpen(true)}
              fontFamily={`'Work Sans Variable', sans-serif`}
              fontSize="14px"
              padding="4px 13px 7.5px 9px"
            />

            <ExportSearchFilterTables
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

        {isCreateModalOpen && mostCommonConnection && (
          <CreateExportTableModal
            connection={mostCommonConnection ? mostCommonConnection : null}
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}

        {data && Array.isArray(data.tables) ? (
          <>
            <p className="list-rows-info">
              {`Showing ${filteredData.length} (of ${data.headersRowInfo.contentRows}) rows`}
            </p>
            <ExportCnTables
              data={filteredData}
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

export default Export
