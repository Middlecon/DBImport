import {
  AirflowTask,
  BaseAirflowDAG,
  Column,
  CustomAirflowDAG,
  EditSetting,
  ExportAirflowDAG,
  ImportAirflowDAG,
  AirflowWithDynamicKeys
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useAirflowDAG } from '../../utils/queries'
import EditTableModal from '../../components/EditTableModal'
// import { useQueryClient } from '@tanstack/react-query'
// import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'
import '../../components/Loading.scss'
import { airflowTaskRowDataEdit } from '../../utils/cardRenderFormatting'
import {
  updateCustomDagData,
  updateExportDagData,
  updateImportDagData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateAirflowDag } from '../../utils/mutations'

function AirflowTasks({ type }: { type: string }) {
  const { dagName } = useParams<{
    dagName: string
  }>()

  const { data: originalDagData, isLoading } = useAirflowDAG(type, dagName)
  const queryClient = useQueryClient()
  const { mutate: updateDag } = useUpdateAirflowDag()

  // const queryClient = useQueryClient()
  // const { mutate: updateTable } = useUpdateTable()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [dataRefreshTrigger, setDataRefreshTrigger] = useState(0)
  const tasksData = useMemo(
    () => [...(originalDagData?.tasks || [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [originalDagData, dataRefreshTrigger]
  )
  // const tasksData: AirflowTask[] = originalDagData.tasks

  const columns: Column<AirflowTask>[] = useMemo(
    () => [
      { header: 'Task Name', accessor: 'name' },
      { header: 'Type', accessor: 'type' },
      { header: 'Placement', accessor: 'placement' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Airflow Pool', accessor: 'airflowPool' },
      { header: 'Airflow Priority', accessor: 'airflowPriority' },
      {
        header: 'Include In Airflow',
        accessor: 'includeInAirflow'
      },
      {
        header: 'Task Dependency Downstream',
        accessor: 'taskDependencyDownstream'
      },
      {
        header: 'Task Dependency Upstream',
        accessor: 'taskDependencyUpstream'
      },
      { header: 'Task Config', accessor: 'taskConfig' },
      {
        header: 'Sensor Poke Interval',
        accessor: 'sensorPokeInterval'
      },
      {
        header: 'Sensor Timeout Minutes',
        accessor: 'sensorTimeoutMinutes'
      },
      {
        header: 'Sensor Connection',
        accessor: 'sensorConnection'
      },
      { header: 'Sensor Soft Fail', accessor: 'sensorSoftFail' },
      { header: 'Sudo User', accessor: 'sudoUser' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: AirflowTask) => {
      if (!originalDagData) {
        console.error('Table data is not available.')
        return
      }

      const rowData: EditSetting[] = airflowTaskRowDataEdit(row)

      setCurrentRow(rowData)
      setIsEditModalOpen(true)
    },
    [originalDagData]
  )

  if (!originalDagData) return <div className="loading">No data found yet.</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    const dagDataCopy = { ...originalDagData }

    let editedDagData:
      | ImportAirflowDAG
      | BaseAirflowDAG
      | ExportAirflowDAG
      | null = null
    if (type === 'import') {
      editedDagData = updateImportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ImportAirflowDAG>,
        updatedSettings,
        true
      )
    } else if (type === 'export') {
      editedDagData = updateExportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ExportAirflowDAG>,
        updatedSettings,
        true
      )
    } else if (type === 'custom') {
      editedDagData = updateCustomDagData(
        dagDataCopy as AirflowWithDynamicKeys<CustomAirflowDAG>,
        updatedSettings,
        true
      )
    }

    if (editedDagData && type) {
      queryClient.setQueryData(['airflows', type, dagName], editedDagData)
      updateDag(
        { type, dagData: editedDagData },
        {
          onSuccess: (response) => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', type, dagName]
            }) // For getting fresh data from database to the cache
            setDataRefreshTrigger((prev) => prev + 1)
            console.log('Update successful', response)
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(
              ['airflows', type, dagName],
              originalDagData
            )

            console.error('Error updating table', error)
          }
        }
      )
    }
  }

  return (
    <div style={{ marginTop: 40 }}>
      {tasksData.length > 0 ? (
        <TableList
          columns={columns}
          data={tasksData}
          onEdit={handleEditClick}
          isLoading={isLoading}
          scrollbarMarginTop="64px"
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
          No Tasks yet in this DAG.
        </p>
      )}

      {isEditModalOpen && currentRow && (
        <EditTableModal
          title={`Edit DAG ${currentRow[0].value}`}
          settings={currentRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default AirflowTasks
