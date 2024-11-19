import { useLocation, useNavigate } from 'react-router-dom'
import { useSearchExportTables } from '../../utils/queries'
import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import { EditSetting } from '../../utils/interfaces'
import { useAtom } from 'jotai'
import {
  exportCnListFiltersAtom,
  exportPersistStateAtom
} from '../../atoms/atoms'
import CreateExportTableModal from '../../components/CreateExportTableModal'
import { createExportTableData } from '../../utils/dataFunctions'
import { useCreateExportTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
import ExportCnTables from './ExportCnTables'
import ExportSearchFilterTables from '../../components/ExportSearchFilterTables'

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
    title: 'Last update from source',
    accessor: 'lastUpdateFromHive  ',
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

function Export() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['connection', 'targetTable', 'targetSchema']
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

  const cleanConnection = connection ? connection.replace(/\*/g, '') : null

  const { data: tables, isLoading: isSearchLoading } = useSearchExportTables(
    connection,
    targetSchema,
    targetTable
  )

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

  const handleShow = (
    connection: string | null,
    targetTable: string | null,
    targetSchema: string | null
  ) => {
    const params = new URLSearchParams(location.search)

    if (connection && connection !== null) {
      params.set('connection', connection)
    } else {
      params.delete('connection')
    }

    if (targetTable && targetTable !== null) {
      params.set('targetTable', targetTable)
    } else {
      params.delete('targetTable')
    }

    if (targetSchema && targetSchema !== null) {
      params.set('targetSchema', targetSchema)
    } else {
      params.delete('targetSchema')
    }

    const connectionUrlString =
      connection !== null
        ? `connection=${params.get('connection') || null}`
        : null
    const targetTableUrlString =
      targetTable !== null
        ? `targetTable=${params.get('targetTable') || null}`
        : null
    const targetSchemaUrlString =
      targetSchema !== null
        ? `targetSchema=${params.get('targetSchema') || null}`
        : null

    const orderedSearch = [
      connectionUrlString,
      targetTableUrlString,
      targetSchemaUrlString
    ]
      .filter((param) => param !== null) // Remove null values
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setExportPersistState(`/export?${orderedSearch}`)

      navigate(`/export?${orderedSearch}`, { replace: true })

      queryClient.invalidateQueries({
        queryKey: ['export', 'search', connection, targetTable, targetSchema]
      })
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
    if (!tables || tables.length < 1) return null
    const connectionCounts = tables.reduce((acc, row) => {
      const connection = row.connection
      if (connection) {
        acc[connection] = (acc[connection] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    return Object.keys(connectionCounts).reduce((a, b) =>
      connectionCounts[a] > connectionCounts[b] ? a : b
    )
  }, [tables])

  const handleSave = (newTableData: EditSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createExportTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: [
            'export',
            'search',
            newTable.connection,
            newTable.targetSchema,
            newTable.targetTable
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
    if (!Array.isArray(tables)) return []
    return tables.filter((row) => {
      const rowDate = parseTimestamp(row.lastUpdateFromHive)

      return [...checkboxFilters, ...radioFilters].every((filter) => {
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
  }, [tables, selectedFilters, getDateRange])

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
          {checkboxFilters.map((filter, index) => (
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
              onToggle={(isOpen) =>
                handleDropdownToggle(filter.accessor, isOpen)
              }
            />
          ))}
        </div>

        {isCreateModalOpen && mostCommonConnection && (
          <CreateExportTableModal
            connection={cleanConnection ? cleanConnection : null}
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}

        {Array.isArray(tables) ? (
          <ExportCnTables data={filteredData} isLoading={isSearchLoading} />
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
