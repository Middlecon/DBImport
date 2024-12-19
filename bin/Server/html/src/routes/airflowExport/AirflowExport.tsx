import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useExportAirflows } from '../../utils/queries'
import {
  AirflowsExportData,
  BulkUpdateAirflowDAG,
  Column,
  EditSetting,
  UiAirflowsExportData,
  UiAirflowsSearchFilter,
  UiBulkAirflowDAG
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
import { useAtom } from 'jotai'
import { airflowExportDagsPersistStateAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import CreateAirflowModal from '../../components/CreateAirflowModal'
import { createExportDagData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteAirflowDags,
  useBulkUpdateAirflowDag,
  useCreateAirflowDag,
  useDeleteAirflowDAG
} from '../../utils/mutations'
import ConfirmationModal from '../../components/ConfirmationModal'
import BulkEditModal from '../../components/BulkEditModal'
import ListRowsInfo from '../../components/ListRowsInfo'
import { bulkAirflowDagFieldsData } from '../../utils/cardRenderFormatting'
import { useLocation, useNavigate } from 'react-router-dom'
import AirflowSearchFilterDags from '../../components/AirflowSearchFilterDags'
import { wildcardMatch } from '../../utils/functions'

// const checkboxFilters = [
//   {
//     title: 'Auto Regenerate DAG',
//     accessor: 'autoRegenerateDag',
//     values: ['True', 'False']
//   }
// ]

function AirflowExport() {
  const queryClient = useQueryClient()

  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['name', 'scheduleInterval', 'autoRegenerateDag']
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

  const name = query.get('name') || null
  const scheduleInterval = query.get('scheduleInterval') || null
  const autoRegenerateDag = query.get('autoRegenerateDag') || null

  const { data, isLoading } = useExportAirflows()
  const { mutate: createDAG } = useCreateAirflowDag()
  const { mutate: deleteDAG } = useDeleteAirflowDAG()

  const { mutate: bulkUpdateDag } = useBulkUpdateAirflowDag()
  const { mutate: bulkDeleteAirflowDags } = useBulkDeleteAirflowDags()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentDeleteRow, setCurrentDeleteRow] =
    useState<UiAirflowsExportData>()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [currentRowsBulk, setCurrentRowsBulk] = useState<
    UiBulkAirflowDAG[] | []
  >([])
  const [rowSelection, setRowSelection] = useState({})

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)

  const [, setAirfloExportDagsPersistState] = useAtom(
    airflowExportDagsPersistStateAtom
  )

  // const [selectedFilters, setSelectedFilters] = useAtom(airflowExportFilterAtom)

  const columns: Column<AirflowsExportData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Filter Connection', accessor: 'filterConnection' },
      { header: 'Filter Target Schema', accessor: 'filterTargetSchema' },
      { header: 'Filter Target Table', accessor: 'filterTargetTable' },
      { header: 'Links', isLink: 'airflowLink' }
      // { header: 'Actions', isAction: 'delete' }
    ],
    []
  )

  const bulkAirflowDagFields = bulkAirflowDagFieldsData('export')

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

  const handleShow = (uiFilters: UiAirflowsSearchFilter) => {
    const params = new URLSearchParams(location.search)
    console.log('uiFilters', uiFilters)

    const filterKeys: (keyof UiAirflowsSearchFilter)[] = [
      'name',
      'scheduleInterval',
      'autoRegenerateDag'
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
      setAirfloExportDagsPersistState(`/airflow/export?${orderedSearch}`)
      navigate(`/airflow/export?${orderedSearch}`, { replace: true })
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleDeleteIconClick = (row: UiAirflowsExportData) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
  }

  const handleDelete = async (row: UiAirflowsExportData) => {
    setShowDeleteConfirmation(false)

    const { name: nameDelete } = row

    deleteDAG(
      {
        type: 'export',
        dagName: nameDelete
      },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'export'], // Matches all related queries that starts the queryKey with 'airflows', 'export'
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

  const handleSave = (newExportAirflowSettings: EditSetting[]) => {
    const newExportAirflowData = createExportDagData(newExportAirflowSettings)

    createDAG(
      { type: 'export', dagData: newExportAirflowData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'export']
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

    return data.filter((row) => {
      const matchesName = wildcardMatch(name, row.name)
      const matchesScheduleInterval = wildcardMatch(
        scheduleInterval,
        row.scheduleInterval
      )
      const matchesAutoRegenerateDag = autoRegenerateDag
        ? row.autoRegenerateDagDisplay === autoRegenerateDag
        : true

      return matchesName && matchesScheduleInterval && matchesAutoRegenerateDag
    })
  }, [data, name, scheduleInterval, autoRegenerateDag])

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
      { type: 'export', dagData: bulkUpdateJson },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'export']
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
      { type: 'export', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'export'], // Matches all related queries that starts the queryKey with 'airflows', 'import'
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
          <h1>Airflow Export</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create"
              onClick={() => setCreateModalOpen(true)}
              fontSize="14px"
            />
            <AirflowSearchFilterDags
              isSearchFilterOpen={openDropdown === 'searchFilter'}
              type="export"
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
              airflowType="export"
              rowSelection={rowSelection}
              onRowSelectionChange={setRowSelection}
              enableMultiSelection={true}
            />
          </>
        ) : isLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="import-text-block">
            <p>No export DAGs yet.</p>
          </div>
        )}
        {isCreateModalOpen && (
          <CreateAirflowModal
            isCreateModalOpen={isCreateModalOpen}
            type="export"
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

export default AirflowExport
