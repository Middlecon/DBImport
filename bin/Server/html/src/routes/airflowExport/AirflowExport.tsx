import '../import/Import.scss'
import { useCallback, useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useExportAirflows } from '../../utils/queries'
import {
  AirflowsExportData,
  BulkUpdateAirflowDAG,
  Column,
  UiAirflowsExportData,
  UiBulkAirflowDAG
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'
import Button from '../../components/Button'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteAirflowDags,
  useBulkUpdateAirflowDag,
  useGenDagConnection
} from '../../utils/mutations'
import ConfirmationModal from '../../components/ConfirmationModal'
import BulkEditModal from '../../components/BulkEditModal'
import ListRowsInfo from '../../components/ListRowsInfo'
import {
  bulkAirflowDagFieldsData,
  generateDagSettings
} from '../../utils/cardRenderFormatting'
import { useLocation, useNavigate } from 'react-router-dom'
import { wildcardMatch } from '../../utils/functions'
import AirflowExportActions from './AirflowExportActions'
import { AxiosError } from 'axios'
import EditTableModal from '../../components/EditTableModal'
import { ErrorData } from '../connection/connectionDetailed/ConnectionDetailedView'

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

  const { data: dags, isLoading } = useExportAirflows()

  const [isGenDagModalOpen, setIsGenDagModalOpen] = useState(false)
  const [isGenDagLoading, setIsGenDagLoading] = useState(false)
  const [errorMessageGenerate, setErrorMessageGenerate] = useState<
    string | null
  >(null)
  const [successMessageGenerate, setSuccessMessageGenerate] = useState<
    string | null
  >(null)

  const [selectedGenerateRow, setSelectedGenerateRow] =
    useState<UiAirflowsExportData>()

  const settings = generateDagSettings(
    selectedGenerateRow ? selectedGenerateRow.name : ''
  )

  const { mutate: generateDag } = useGenDagConnection()

  const { mutate: bulkUpdateDag } = useBulkUpdateAirflowDag()
  const { mutate: bulkDeleteAirflowDags } = useBulkDeleteAirflowDags()

  const [selectedRowsBulk, setSelectedRowsBulk] = useState<
    UiBulkAirflowDAG[] | []
  >([])
  const [rowSelection, setRowSelection] = useState({})

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)

  // const [selectedFilters, setSelectedFilters] = useAtom(airflowExportFilterAtom)

  const columns: Column<AirflowsExportData>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
      { header: 'Schedule Interval', accessor: 'scheduleInterval' },
      { header: 'Auto Regenerate DAG', accessor: 'autoRegenerateDag' },
      { header: 'Filter Connection', accessor: 'filterConnection' },
      { header: 'Filter Target Schema', accessor: 'filterTargetSchema' },
      { header: 'Filter Target Table', accessor: 'filterTargetTable' },
      { header: 'Links', isLink: 'airflowLink' },
      { header: 'Actions', isAction: 'generateDag' }
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

  const handleGenerateIconClick = (row: UiAirflowsExportData) => {
    setSelectedGenerateRow(row)
    setIsGenDagModalOpen(true)
  }

  const handleGenerateDag = () => {
    if (!selectedGenerateRow) {
      console.log('No selected row or DAG name', selectedGenerateRow)
      return
    }

    setIsGenDagLoading(true)

    generateDag(selectedGenerateRow.name, {
      onSuccess: (response) => {
        setIsGenDagLoading(false)
        setSuccessMessageGenerate('Generate DAG succeeded')
        console.log('Generating DAG succeeded, result:', response)
        // setIsGenDagModalOpen(false)
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.statusText &&
          error.response?.data.detail !== undefined
            ? `${error.message} ${error.response?.statusText}, ${error.response?.data.detail[0].msg}: ${error.response?.data.detail[0].type}`
            : error.status === 500
            ? `${error.message} ${error.response?.statusText}: ${error.response?.data}`
            : 'An unknown error occurred'
        setIsGenDagLoading(false)
        setErrorMessageGenerate(errorMessage)
        console.log('error', error)
        console.error('Generate DAG failed', error.message)
      }
    })
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
    console.log('handleBulkEditClick selectedRows', selectedRows)
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

  return (
    <>
      <ViewBaseLayout>
        <div className="header-container">
          <h1>Airflow Export</h1>
          <AirflowExportActions dags={dags} />
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
              airflowType="export"
              rowSelection={rowSelection}
              onRowSelectionChange={setRowSelection}
              enableMultiSelection={true}
            />
          </>
        ) : isLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="text-block">
            <p>No export DAGs yet.</p>
          </div>
        )}

        {isBulkEditModalOpen && (
          <BulkEditModal
            isBulkEditModalOpen={isBulkEditModalOpen}
            title={`Edit the ${selectedRowsLength} selected DAG${
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
          <ConfirmationModal
            title={`Delete the ${selectedRowsLength} selected DAG${
              selectedRowsLength > 1 ? 's' : ''
            }`}
            message={`Are you sure that you want to delete the ${selectedRowsLength} selected DAG${
              selectedRowsLength > 1 ? 's' : ''
            }? \nDelete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleBulkDelete(selectedRowsBulk)}
            onCancel={() => setShowBulkDeleteConfirmation(false)}
            isActive={showBulkDeleteConfirmation}
          />
        )}

        {isGenDagModalOpen && (
          <EditTableModal
            isEditModalOpen={isGenDagModalOpen}
            title={`Generate DAG`}
            settings={settings}
            onSave={handleGenerateDag}
            onClose={() => {
              setIsGenDagModalOpen(false)
              setErrorMessageGenerate(null)
              setSuccessMessageGenerate(null)
            }}
            isNoCloseOnSave={true}
            initWidth={300}
            isLoading={isGenDagLoading}
            loadingText="Generating"
            successMessage={successMessageGenerate}
            errorMessage={errorMessageGenerate ? errorMessageGenerate : null}
            onResetErrorMessage={() => setErrorMessageGenerate(null)}
            submitButtonTitle="Generate"
            closeButtonTitle="Close"
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowExport
