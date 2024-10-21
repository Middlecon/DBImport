import '../import/Import.scss'
import { useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useImportAirflows } from '../../utils/queries'
import { AirflowsImportData, Column } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import { useAtom } from 'jotai'
import { airflowImportFilterAtom } from '../../atoms/atoms'

const checkboxFilters = [
  {
    title: 'Auto Regenerate DAG',
    accessor: 'autoRegenerateDag',
    values: ['True', 'False']
  }
]

function AirflowImport() {
  const { data, isLoading } = useImportAirflows()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [selectedFilters, setSelectedFilters] = useAtom(airflowImportFilterAtom)

  const columns: Column<AirflowsImportData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Filter Table', accessor: 'filterTable' }
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
    if (!Array.isArray(data)) return []
    return data.filter((row) => {
      return [...checkboxFilters].every((filter) => {
        const selectedItems = Array.isArray(selectedFilters[filter.accessor])
          ? selectedFilters[filter.accessor]?.map((value) => {
              if (value === 'True') return true
              if (value === 'False') return false
              return value
            })
          : []

        if (selectedItems.length === 0) return true

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters])

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Airflow Import</h1>
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

export default AirflowImport
