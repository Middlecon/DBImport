import '../import/Import.scss'
import { useCallback, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useImportAirflows } from '../../utils/queries'
import {
  AirflowsImportData,
  Column,
  EditSetting,
  UiAirflowsImportData
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import { useAtom } from 'jotai'
import { airflowImportFilterAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import CreateAirflowModal from '../../components/CreateAirflowModal'
import { createImportDagData } from '../../utils/dataFunctions'
import { useCreateAirflowDag, useDeleteAirflowDAG } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import ConfirmationModal from '../../components/ConfirmationModal'

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
  const { mutate: deleteDAG } = useDeleteAirflowDAG()

  const queryClient = useQueryClient()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentDeleteRow, setCurrentDeleteRow] =
    useState<UiAirflowsImportData>()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

  const [selectedFilters, setSelectedFilters] = useAtom(airflowImportFilterAtom)

  const columns: Column<AirflowsImportData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Filter Table', accessor: 'filterTable' },
      { header: 'Actions', isAction: 'delete' }
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

  const handleDeleteIconClick = (row: UiAirflowsImportData) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
  }

  const handleDelete = async (row: UiAirflowsImportData) => {
    setShowDeleteConfirmation(false)

    const { name: nameDelete } = row

    deleteDAG(
      {
        type: 'import',
        dagName: nameDelete
      },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'import'], // Matches all related queries that starts the queryKey with 'airflows', 'import'
            exact: false
          })
          console.log('Delete successful')
        },
        onError: (error) => {
          console.error('Error deleting item', error)
        }
      }
    )
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

  const handleBulkEditClick = useCallback(() => {
    if (!data) return
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    console.log('handleBulkEditClick selectedRows', selectedRows)
    // setCurrentRowsBulk(selectedRows)
    // setIsBulkEditModalOpen(true)
  }, [rowSelection, data])

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
        <div
          className="list-top-info-and-edit"
          style={{ visibility: 'hidden' }}
        >
          <Button title="Bulk Edit" onClick={handleBulkEditClick} />
        </div>
        {filteredData ? (
          <TableList
            columns={columns}
            data={filteredData}
            isLoading={isLoading}
            onDelete={handleDeleteIconClick}
            airflowType="import"
            rowSelection={rowSelection}
            onRowSelectionChange={setRowSelection}
            enableMultiSelection={true}
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
        {showDeleteConfirmation && currentDeleteRow && (
          <ConfirmationModal
            title={`Delete ${currentDeleteRow.name}`}
            message={`Are you sure that you want to delete \n\n DAG "${currentDeleteRow.name}"? Delete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleDelete(currentDeleteRow)}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowImport
