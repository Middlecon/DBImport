import '../import/Import.scss'
import { useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useConnections } from '../../utils/queries'
import { Column, Connections } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'

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
  const { data: connectionsData, isLoading } = useConnections()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [selectedFilters, setSelectedFilters] = useState<{
    [key: string]: string[]
  }>({})

  const columns: Column<Connections>[] = useMemo(
    () => [
      { header: 'Connection Name', accessor: 'name' },
      { header: 'Server Type', accessor: 'serverType' },
      { header: 'Connection String', accessor: 'connectionString' }
    ],
    []
  )

  const handleSelect = (filterKey: string, items: string[]) => {
    setSelectedFilters((prevFilters) => ({
      ...prevFilters,
      [filterKey]: items
    }))
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const filteredData = useMemo(() => {
    if (!connectionsData) return []
    return connectionsData.filter((row) => {
      return [...checkboxFilters].every((filter) => {
        const selectedItems =
          selectedFilters[filter.accessor]?.map((value) => value) || []

        if (selectedItems.length === 0) return true

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [connectionsData, selectedFilters])

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Connection']}>
        <div className="import-header">
          <h1>Connection</h1>
        </div>

        <div className="filters">
          {checkboxFilters.map((filter, index) => (
            <DropdownCheckbox
              key={index}
              items={filter.values}
              title={filter.title}
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
