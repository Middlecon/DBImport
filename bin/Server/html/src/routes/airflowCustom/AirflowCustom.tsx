import '../import/Import.scss'
import { useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useCustomAirflows } from '../../utils/queries'
import { AirflowsCustomData, Column, EditSetting } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import { useAtom } from 'jotai'
import { airflowCustomFilterAtom } from '../../atoms/atoms'
import CreateAirflowModal from '../../components/CreateAirflowModal'
import Button from '../../components/Button'
import { createCustomDagData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useCreateAirflowDag } from '../../utils/mutations'

const checkboxFilters = [
  {
    title: 'Auto Regenerate DAG',
    accessor: 'autoRegenerateDag',
    values: ['True', 'False']
  }
]

function AirflowCustom() {
  const { data, isLoading } = useCustomAirflows()
  const { mutate: createDAG } = useCreateAirflowDag()
  const queryClient = useQueryClient()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [selectedFilters, setSelectedFilters] = useAtom(airflowCustomFilterAtom)

  const columns: Column<AirflowsCustomData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' }
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

  const handleSave = (newCustomAirflowSettings: EditSetting[]) => {
    const newCustomAirflowData = createCustomDagData(newCustomAirflowSettings)

    if (newCustomAirflowData) {
      createDAG(
        { type: 'custom', dagData: newCustomAirflowData },
        {
          onSuccess: (response) => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', 'custom']
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
          <h1>Airflow Custom</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create"
              onClick={() => setCreateModalOpen(true)}
              fontFamily={`'Work Sans Variable', sans-serif`}
              fontSize="14px"
              padding="4px 13px 7.5px 9px"
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
            airflowType="custom"
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
        {isCreateModalOpen && (
          <CreateAirflowModal
            type="custom"
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowCustom
