import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useImportAirflows } from '../../utils/queries'
import {
  AirflowsImportData,
  BulkUpdateAirflowDAG,
  Column,
  EditSetting,
  UiAirflowsImportData,
  UiAirflowsSearchFilter,
  UiBulkAirflowDAG
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
// import { useAtom } from 'jotai'
// import { airflowImportFilterAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import CreateAirflowModal from '../../components/CreateAirflowModal'
import { createImportDagData } from '../../utils/dataFunctions'
import {
  useBulkDeleteAirflowDags,
  useBulkUpdateAirflowDag,
  useCreateAirflowDag,
  useDeleteAirflowDAG
} from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import ConfirmationModal from '../../components/ConfirmationModal'
import ListRowsInfo from '../../components/ListRowsInfo'
import BulkEditModal from '../../components/BulkEditModal'
import { bulkAirflowDagFieldsData } from '../../utils/cardRenderFormatting'
import { useLocation, useNavigate } from 'react-router-dom'
import AirflowSearchFilterDags from '../../components/AirflowSearchFilterDags'
import { airflowImportDagsPersistStateAtom } from '../../atoms/atoms'
import { useAtom } from 'jotai'

// const checkboxFilters = [
//   {
//     title: 'Auto Regenerate DAG',
//     accessor: 'autoRegenerateDag',
//     values: ['True', 'False']
//   }
// ]

function AirflowImport() {
  const queryClient = useQueryClient()

  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['autoRegenerateDag']
  const allParams = Array.from(query.keys())

  useEffect(() => {
    console.log('allParams', allParams)
    const hasInvalidParams = allParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/airflow/import', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allParams, navigate])

  const autoRegenerateDag = query.get('autoRegenerateDag') || null

  const { data, isLoading } = useImportAirflows()
  const { mutate: createDAG } = useCreateAirflowDag()
  const { mutate: deleteDAG } = useDeleteAirflowDAG()

  const { mutate: bulkUpdateDag } = useBulkUpdateAirflowDag()
  const { mutate: bulkDeleteAirflowDags } = useBulkDeleteAirflowDags()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentDeleteRow, setCurrentDeleteRow] =
    useState<UiAirflowsImportData>()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [currentRowsBulk, setCurrentRowsBulk] = useState<
    UiBulkAirflowDAG[] | []
  >([])
  const [rowSelection, setRowSelection] = useState({})

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)

  const [, setAirflowImportDagsPersistState] = useAtom(
    airflowImportDagsPersistStateAtom
  )

  // const [selectedFilters, setSelectedFilters] = useAtom(airflowImportFilterAtom)

  const columns: Column<AirflowsImportData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Filter Table', accessor: 'filterTable' }
      // { header: 'Actions', isAction: 'delete' }
    ],
    []
  )

  const bulkAirflowDagFields = bulkAirflowDagFieldsData('import')

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

  const handleShow = (uiFilters: UiAirflowsSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiAirflowsSearchFilter)[] = ['autoRegenerateDag']

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
      setAirflowImportDagsPersistState(`/airflow/import?${orderedSearch}`)
      navigate(`/airflow/import?${orderedSearch}`, { replace: true })
    }
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

  const filteredData = useMemo(() => {
    if (!data || !Array.isArray(data)) return []
    console.log('autoRegenerateDag', autoRegenerateDag)
    return data.filter((row) => {
      if (autoRegenerateDag === null) return true

      return row.autoRegenerateDagDisplay === autoRegenerateDag
    })
  }, [data, autoRegenerateDag])

  // const filteredData = useMemo(() => {
  //   if (!Array.isArray(data)) return []
  //   console.log('filteredData data', data)
  //   return data.filter((row) => {
  //     return [...checkboxFilters].every((filter) => {
  //       const selectedItems = Array.isArray(selectedFilters[filter.accessor])
  //         ? selectedFilters[filter.accessor]?.map((value) => value)
  //         : []

  //       if (selectedItems.length === 0) return true

  //       const accessorKey = filter.accessor as keyof typeof row
  //       const displayKey = `${String(accessorKey)}Display` as keyof typeof row
  //       const rowValue = (row[displayKey] ?? row[accessorKey]) as string

  //       return selectedItems.includes(rowValue)
  //     })
  //   })
  // }, [data, selectedFilters])

  const handleBulkEditClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => filteredData[index])
    console.log('handleBulkEditClick selectedRows', selectedRows)
    setCurrentRowsBulk(selectedRows)
    setIsBulkEditModalOpen(true)
  }, [rowSelection, filteredData])

  const handleBulkEditSave = (
    bulkChanges: Record<string, string | number | boolean | null>
  ) => {
    const airflowDagPks = currentRowsBulk.map((row) => ({
      name: row.name
    }))

    const bulkUpdateJson: BulkUpdateAirflowDAG = {
      ...bulkChanges,
      scheduleInterval:
        bulkChanges.scheduleInterval === null
          ? ''
          : (bulkChanges.scheduleInterval as string | null),
      dags: airflowDagPks
    }

    bulkUpdateDag(
      { type: 'import', dagData: bulkUpdateJson },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'import'], // Matches all related queries that starts the queryKey with 'airflows', 'import'
            exact: false
          })
          console.log('Update successful', response)
          setIsBulkEditModalOpen(false)
        },
        onError: (error) => {
          console.error('Error updating table', error)
        }
      }
    )
  }

  const handleBulkDeleteClick = useCallback(() => {
    if (!data) return
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    setCurrentRowsBulk(selectedRows)
    setShowBulkDeleteConfirmation(true)
  }, [rowSelection, data])

  const handleBulkDelete = async (rows: UiBulkAirflowDAG[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(({ name }) => ({
      name
    }))

    console.log(bulkDeleteRowsPks)

    bulkDeleteAirflowDags(
      { type: 'import', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'import'], // Matches all related queries that starts the queryKey with 'airflows', 'import'
            exact: false
          })
          console.log('Delete successful')
          setRowSelection({})
        },
        onError: (error: Error) => {
          console.error('Error deleting item', error)
        }
      }
    )
  }

  const currentRowsBulkData = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => filteredData[index])
  }, [rowSelection, filteredData])

  const currentRowsLength = useMemo(
    () => currentRowsBulkData.length,
    [currentRowsBulkData]
  )

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Airflow Import</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create"
              onClick={() => setCreateModalOpen(true)}
              fontSize="14px"
            />
            <AirflowSearchFilterDags
              isSearchFilterOpen={openDropdown === 'searchFilter'}
              type="import"
              onToggle={(isSearchFilterOpen: boolean) =>
                handleDropdownToggle('searchFilter', isSearchFilterOpen)
              }
              onShow={handleShow}
              disabled={!data}
            />
          </div>
        </div>

        {/* <div className="filters">
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
        </div> */}

        {filteredData && Array.isArray(data) && data.length > 0 ? (
          <>
            <div className="list-top-info-and-edit">
              <Button
                title={`Edit ${
                  currentRowsLength > 0 ? currentRowsLength : ''
                } DAG${
                  currentRowsLength > 1 || currentRowsLength === 0 ? 's' : ''
                }`}
                onClick={handleBulkEditClick}
                disabled={currentRowsLength < 1}
              />
              <Button
                title={`Delete ${
                  currentRowsLength > 0 ? currentRowsLength : ''
                } DAG${
                  currentRowsLength > 1 || currentRowsLength === 0 ? 's' : ''
                }`}
                onClick={handleBulkDeleteClick}
                deleteStyle={true}
                disabled={currentRowsLength < 1}
              />
            </div>
            <ListRowsInfo
              filteredData={filteredData}
              contentTotalRows={String(data.length)}
              itemType="DAG"
            />
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
          </>
        ) : isLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="import-text-block">
            <p>No import DAGs yet.</p>
          </div>
        )}
        {isCreateModalOpen && (
          <CreateAirflowModal
            isCreateModalOpen={isCreateModalOpen}
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
        {isBulkEditModalOpen && (
          <BulkEditModal
            isBulkEditModalOpen={isBulkEditModalOpen}
            title={`Edit the ${currentRowsLength} selected DAG${
              currentRowsLength > 1 ? 's' : ''
            }`}
            selectedRows={currentRowsBulk}
            bulkFieldsData={bulkAirflowDagFields}
            onSave={handleBulkEditSave}
            onClose={() => setIsBulkEditModalOpen(false)}
            initWidth={684}
          />
        )}
        {showBulkDeleteConfirmation && currentRowsBulk && (
          <ConfirmationModal
            title={`Delete the ${currentRowsLength} selected DAG${
              currentRowsLength > 1 ? 's' : ''
            }`}
            message={`Are you sure that you want to delete the ${currentRowsLength} selected DAG${
              currentRowsLength > 1 ? 's' : ''
            }? \nDelete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleBulkDelete(currentRowsBulk)}
            onCancel={() => setShowBulkDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowImport
