import '../import/Import.scss'
import { useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useImportAirflows } from '../../utils/queries'
import { AirflowsImportData, Column, EditSetting } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import { useAtom } from 'jotai'
import { airflowImportFilterAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import CreateAirflowModal from '../../components/CreateAirflowModal'
import { createImportDagData } from '../../utils/dataFunctions'
import { useCreateAirflowDag } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'

const checkboxFilters = [
  {
    title: 'Auto Regenerate DAG',
    accessor: 'autoRegenerateDag',
    values: ['True', 'False']
  }
]

function AirflowImport() {
  const { data, isLoading } = useImportAirflows()
  const { mutate: createDAG } = useCreateAirflowDag()
  const queryClient = useQueryClient()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

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

  const handleSave = (newImportAirflowSettings: EditSetting[]) => {
    const newImportAirflowData = createImportDagData(newImportAirflowSettings)

    createDAG(
      { type: 'import', dagData: newImportAirflowData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'import']
          })
          console.log('Update successful', response)
          setCreateModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  const filteredData = useMemo(() => {
    if (!Array.isArray(data)) return []
    return data.filter((row) => {
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
  }, [data, selectedFilters])

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Airflow Import</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create"
              onClick={() => setCreateModalOpen(true)}
              fontFamily={`'Work Sans Variable', sans-serif`}
              fontSize="14px"
              padding="4px 12px 7.5px 9px"
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
            scrollbarMarginTop="64px"
            airflowType="import"
          />
        ) : (
          <div>Loading....</div>
        )}
        {isCreateModalOpen && (
          <CreateAirflowModal
            type="import"
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowImport
