import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useAirflowDAG, useCustomAirflows } from '../../utils/queries'
import {
  AirflowsCustomData,
  BulkUpdateAirflowDAG,
  Column,
  UiAirflowsCustomData,
  UiBulkAirflowDAG
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
import Button from '../../components/Button'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteAirflowDags,
  useBulkUpdateAirflowDag,
  useCreateAirflowDag,
  useDeleteAirflowDAG
} from '../../utils/mutations'

import ListRowsInfo from '../../components/ListRowsInfo'
import { bulkAirflowDagFieldsData } from '../../utils/cardRenderFormatting'
import { useLocation, useNavigate } from 'react-router-dom'
import { wildcardMatch } from '../../utils/functions'
import AirflowCustomActions from './AirflowCustomActions'
import GenerateDagModal from '../../components/modals/GenerateDagModal'
import BulkEditModal from '../../components/modals/BulkEditModal'
import RenameAirflowModal from '../../components/modals/RenameAirflowModal'
import { newCopyDagData } from '../../utils/dataFunctions'
import DeleteModal from '../../components/modals/DeleteModal'
import { useAtom } from 'jotai'
import { clearRowSelectionAtom } from '../../atoms/atoms'

// const checkboxFilters = [
//   {
//     title: 'Auto Regenerate DAG',
//     accessor: 'autoRegenerateDag',
//     values: ['True', 'False']
//   }
// ]

function AirflowCustom() {
  const queryClient = useQueryClient()

  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['name', 'scheduleInterval', 'autoRegenerateDag']
  const allSearchParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allSearchParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/airflow/custom', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allSearchParams, navigate])

  const name = query.get('name') || null
  const scheduleInterval = query.get('scheduleInterval') || null
  const autoRegenerateDag = query.get('autoRegenerateDag') || null

  const { data: dags, isLoading } = useCustomAirflows()

  const [isGenDagModalOpen, setIsGenDagModalOpen] = useState(false)
  const [isRenameAirflowDagModalOpen, setIsRenameAirflowDagModalOpen] =
    useState(false)

  const { mutate: createDAG } = useCreateAirflowDag()
  const { mutate: deleteDag } = useDeleteAirflowDAG()

  const { mutate: bulkUpdateDag } = useBulkUpdateAirflowDag()
  const { mutate: bulkDeleteAirflowDags } = useBulkDeleteAirflowDags()

  const [selectedRow, setSelectedRow] = useState<UiAirflowsCustomData>()

  const [selectedRowsBulk, setSelectedRowsBulk] = useState<
    UiBulkAirflowDAG[] | []
  >([])
  const [rowSelection, setRowSelection] = useState({})

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)

  // const [selectedFilters, setSelectedFilters] = useAtom(airflowCustomFilterAtom)

  const columns: Column<AirflowsCustomData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Links', isLink: 'airflowLink' },
      { header: 'Actions', isAction: 'generateDagAndRename' }
    ],
    []
  )

  const { data: dagData } = useAirflowDAG(
    'custom',
    selectedRow ? selectedRow.name : undefined
  )

  const bulkAirflowDagFields = bulkAirflowDagFieldsData('custom')

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

  const handleGenerateIconClick = (row: UiAirflowsCustomData) => {
    setSelectedRow(row)
    setIsGenDagModalOpen(true)
  }

  const handleRenameIconClick = (row: UiAirflowsCustomData) => {
    setSelectedRow(row)
    setIsRenameAirflowDagModalOpen(true)
  }

  const handleSaveRename = (newDagName: string) => {
    if (!selectedRow || !dagData) return

    const dagName = selectedRow.name

    const newDagDataCopy = newCopyDagData(newDagName, dagData)

    createDAG(
      { type: 'custom', dagData: newDagDataCopy },
      {
        onSuccess: () => {
          console.log('Save renamned copy successful')

          deleteDag(
            { type: 'custom', dagName },
            {
              onSuccess: () => {
                queryClient.invalidateQueries({
                  queryKey: ['airflows', 'custom'],
                  exact: true
                })

                queryClient.removeQueries({
                  queryKey: ['airflows', 'custom', dagName],
                  exact: true
                })

                console.log('Delete original successful, rename successful')

                setIsRenameAirflowDagModalOpen(false)
              },
              onError: (error) => {
                console.log('Error deleting DAG:', error.message)
              }
            }
          )
        },
        onError: (error) => {
          console.log('Error creating rename:', error.message)
        }
      }
    )
  }

  const filteredData = useMemo(() => {
    if (!dags || !Array.isArray(dags)) return []

    return dags.filter((row) => {
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
  }, [dags, name, scheduleInterval, autoRegenerateDag])

  const handleBulkEditClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => filteredData[index])
    setSelectedRowsBulk(selectedRows)
    setIsBulkEditModalOpen(true)
  }, [rowSelection, filteredData])

  const handleBulkEditSave = (
    bulkChanges: Record<string, string | number | boolean | null>
  ) => {
    const airflowDagPks = selectedRowsBulk.map((row) => ({
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
      { type: 'custom', dagData: bulkUpdateJson },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'custom'], // Matches all related queries that starts the queryKey with 'airflows', 'custom'
            exact: false
          })
          console.log('Update successful')
          setIsBulkEditModalOpen(false)
        },
        onError: (error) => {
          console.log('Error updating', error.message)
        }
      }
    )
  }

  const handleBulkDeleteClick = useCallback(() => {
    if (!dags) return
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => dags[index])
    setSelectedRowsBulk(selectedRows)
    setShowBulkDeleteConfirmation(true)
  }, [rowSelection, dags])

  const handleBulkDelete = async (rows: UiBulkAirflowDAG[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(({ name }) => ({
      name
    }))

    bulkDeleteAirflowDags(
      { type: 'custom', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'custom'], // Matches all related queries that starts the queryKey with 'airflows', 'custom'
            exact: false
          })
          console.log('Delete successful')
          setRowSelection({})
        },
        onError: (error: Error) => {
          console.log('Error deleting', error.message)
        }
      }
    )
  }

  const selectedRowsBulkData = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => filteredData[index])
  }, [rowSelection, filteredData])

  const selectedRowsLength = useMemo(
    () => selectedRowsBulkData.length,
    [selectedRowsBulkData]
  )

  const [clearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  useEffect(() => {
    setRowSelection({})
  }, [clearRowSelectionTrigger])

  return (
    <>
      <ViewBaseLayout>
        <div className="header-container">
          <h1>Airflow Custom</h1>
          <AirflowCustomActions dags={dags} />
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

        {filteredData && Array.isArray(dags) && dags.length > 0 ? (
          <>
            <div className="list-top-info-and-edit">
              <Button
                title={`Edit ${
                  selectedRowsLength > 0 ? selectedRowsLength : ''
                } DAG${
                  selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
                }`}
                onClick={handleBulkEditClick}
                disabled={selectedRowsLength < 1}
              />
              <Button
                title={`Delete ${
                  selectedRowsLength > 0 ? selectedRowsLength : ''
                } DAG${
                  selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
                }`}
                onClick={handleBulkDeleteClick}
                deleteStyle={true}
                disabled={selectedRowsLength < 1}
              />
            </div>
            <ListRowsInfo
              filteredData={filteredData}
              contentTotalRows={String(dags.length)}
              itemType="DAG"
            />
            <TableList
              columns={columns}
              data={filteredData}
              isLoading={isLoading}
              onGenerate={handleGenerateIconClick}
              onRename={handleRenameIconClick}
              airflowType="custom"
              rowSelection={rowSelection}
              onRowSelectionChange={setRowSelection}
              enableMultiSelection={true}
            />
          </>
        ) : isLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="text-block">
            <p>No custom DAGs yet.</p>
          </div>
        )}

        {isBulkEditModalOpen && (
          <BulkEditModal
            isBulkEditModalOpen={isBulkEditModalOpen}
            title={`Edit the ${selectedRowsLength} selected table${
              selectedRowsLength > 1 ? 's' : ''
            }`}
            selectedRows={selectedRowsBulk}
            bulkFieldsData={bulkAirflowDagFields}
            onSave={handleBulkEditSave}
            onClose={() => setIsBulkEditModalOpen(false)}
            initWidth={684}
          />
        )}

        {showBulkDeleteConfirmation && selectedRowsBulk && (
          <DeleteModal
            title={`Delete the ${selectedRowsLength} selected DAG${
              selectedRowsLength > 1 ? 's' : ''
            }`}
            message={`Are you sure that you want to delete the ${selectedRowsLength} selected DAG${
              selectedRowsLength > 1 ? 's' : ''
            }?`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleBulkDelete(selectedRowsBulk)}
            onCancel={() => setShowBulkDeleteConfirmation(false)}
            isActive={showBulkDeleteConfirmation}
          />
        )}
        {isGenDagModalOpen && selectedRow && (
          <GenerateDagModal
            dagName={selectedRow.name}
            isGenDagModalOpen={isGenDagModalOpen}
            onClose={() => setIsGenDagModalOpen(false)}
          />
        )}
        {isRenameAirflowDagModalOpen && selectedRow && (
          <RenameAirflowModal
            type="custom"
            dagName={selectedRow.name}
            isRenameAirflowModalOpen={isRenameAirflowDagModalOpen}
            onSave={handleSaveRename}
            onClose={() => setIsRenameAirflowDagModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowCustom
