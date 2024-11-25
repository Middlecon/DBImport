import '../import/Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useSearchConnections } from '../../utils/queries'
import {
  Column,
  Connections,
  ConnectionSearchFilter
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import {
  connectionFilterAtom,
  connectionPersistStateAtom
} from '../../atoms/atoms'
import { useAtom } from 'jotai'
import { useLocation, useNavigate } from 'react-router-dom'
import ConnectionSearchFilterCns from '../../components/ConnectionSearchFilterCns'

const checkboxFilters = [
  {
    title: 'Server Type',
    accessor: 'serverType',
    values: [
      'MySQL',
      'Oracle',
      'MSSQL Server',
      'PostgreSQL',
      'Progress',
      'DB2 UDB',
      'DB2 AS400',
      'MongoDB',
      'Cache',
      'Snowflake',
      'AWS S3',
      'Informix',
      'SQL Anywhere'
    ]
  }
]

function Connection() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['name', 'connectionString']
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

  const name = query.get('name') || null
  const connectionString = query.get('connectionString') || null

  const filters: ConnectionSearchFilter = useMemo(
    () => ({
      name,
      connectionString
    }),
    [name, connectionString]
  )

  const { data: searchData, isLoading } = useSearchConnections(filters)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [, setConnectionPersistState] = useAtom(connectionPersistStateAtom)
  const [selectedFilters, setSelectedFilters] = useAtom(connectionFilterAtom)

  const handleShow = (uiFilters: ConnectionSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof ConnectionSearchFilter)[] = [
      'name',
      'connectionString'
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
      setConnectionPersistState(`/connection?${orderedSearch}`)
      navigate(`/connection?${orderedSearch}`, { replace: true })
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

  const columns: Column<Connections>[] = useMemo(
    () => [
      { header: 'Connection Name', accessor: 'name' },
      { header: 'Server Type', accessor: 'serverType' },
      { header: 'Connection String', accessor: 'connectionString' },
      { header: 'Links', isAction: 'connectionLink' }
    ],
    []
  )

  const filteredData = useMemo(() => {
    console.log('searchData', searchData)
    if (!searchData || !Array.isArray(searchData.connections)) return []
    return searchData.connections.filter((row) => {
      return [...checkboxFilters].every((filter) => {
        const selectedItems = Array.isArray(selectedFilters[filter.accessor])
          ? selectedFilters[filter.accessor]?.map((value) => value)
          : []

        if (selectedItems.length === 0) return true

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [searchData, selectedFilters])
  console.log('filteredData', filteredData)
  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Connection</h1>
          <div className="db-dropdown">
            <ConnectionSearchFilterCns
              isSearchFilterOpen={openDropdown === 'searchFilter'}
              onToggle={(isSearchFilterOpen: boolean) =>
                handleDropdownToggle('searchFilter', isSearchFilterOpen)
              }
              onShow={handleShow}
            />
          </div>
        </div>

        <div className="filters">
          {Array.isArray(checkboxFilters) &&
            checkboxFilters.map((filter, index) => (
              <DropdownCheckbox
                key={index}
                items={filter.values || []}
                title={filter.title}
                selectedItems={selectedFilters[filter.accessor] || []}
                onSelect={(items) => handleSelect(filter.accessor, items)}
                isOpen={openDropdown === filter.accessor}
                onToggle={(isOpen) =>
                  handleDropdownToggle(filter.accessor, isOpen)
                }
              />
            ))}
        </div>

        {filteredData ? (
          <TableList
            columns={columns}
            data={filteredData}
            isLoading={isLoading}
            scrollbarMarginTop="34px"
          />
        ) : (
          <p
            style={{
              padding: ' 40px 50px 44px 50px',
              backgroundColor: 'white',
              borderRadius: 7,
              textAlign: 'center'
            }}
          >
            No columns yet in this table.
          </p>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Connection
