import { AirflowTask, Column, EditSetting } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useAirflowDAG } from '../../utils/queries'
import EditTableModal from '../../components/EditTableModal'
// import { useQueryClient } from '@tanstack/react-query'
// import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'
import '../../components/Loading.scss'
import { airflowTaskRowDataEdit } from '../../utils/cardRenderFormatting'

function AirflowTasks({ type }: { type: string }) {
  const { dagName } = useParams<{
    dagName: string
  }>()

  const { data: dagData, isFetching, isLoading } = useAirflowDAG(type, dagName)

  // const queryClient = useQueryClient()
  // const { mutate: updateTable } = useUpdateTable()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])

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
      if (!dagData) {
        console.error('Table data is not available.')
        return
      }

      const rowData: EditSetting[] = airflowTaskRowDataEdit(row)

      setCurrentRow(rowData)
      setIsEditModalOpen(true)
    },
    [dagData]
  )

  if (isFetching) return <div className="loading">Loading...</div>
  if (!dagData) return <div>No data found.</div>

  const tasksData: AirflowTask[] = dagData.tasks

  const handleSave = (updatedSettings: EditSetting[]) => {
            setIsEditModalOpen(false)
          },
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
