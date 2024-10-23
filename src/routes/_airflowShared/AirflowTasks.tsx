import { AirflowTask, Column, EditSetting } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useAirflowDAG } from '../../utils/queries'
import EditTableModal from '../../components/EditTableModal'
import { AirflowDAGTaskType, SettingType } from '../../utils/enums'
// import { useQueryClient } from '@tanstack/react-query'
// import { useUpdateTable } from '../../../../utils/mutations'
import { useParams } from 'react-router-dom'
import { getEnumOptions, mapDisplayValue } from '../../utils/nameMappings'

function AirflowTasks({ type }: { type: string }) {
  const { dagName } = useParams<{
    dagName: string
  }>()

  const { data: dagData, isFetching, isLoading } = useAirflowDAG(type, dagName)

  // const queryClient = useQueryClient()
  // const { mutate: updateTable } = useUpdateTable()
  const [isModalOpen, setModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])

  const columns: Column<AirflowTask>[] = useMemo(
    () => [
      { header: 'DAG Name', accessor: 'name' },
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
      { header: 'Sudo User', accessor: 'sudoUser' },
      { header: 'Edit', isAction: 'edit' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: AirflowTask) => {
      if (!dagData) {
        console.error('Table data is not available.')
        return
      }

      console.log('row.type', row.type)

      const rowData: EditSetting[] = [
        {
          label: 'DAG Name',
          value: row.name,
          type: SettingType.Readonly,
          isHidden: true
        }, // Hidden Readonly, have to be unique across import, export and custom, varchar(64), required
        {
          label: 'Type',
          value: mapDisplayValue('type', row.type),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('type')
        }, // Enum, 'shell script','Hive SQL','Hive SQL Script','JDBC SQL','Trigger DAG','DAG Sensor','SQL Sensor','DBImport command', required (default value: 'Hive SQL Script')

        {
          label: 'Placement',
          value: mapDisplayValue('placement', row.placement),
          type: SettingType.Enum,
          enumOptions: getEnumOptions('placement')
        }, // Enum, 'before main','after main','in main', required (default value: 'after main')

        {
          label: 'Connection',
          value: row.connection,
          type: SettingType.ConnectionReference,
          isConditionsMet: row.type === AirflowDAGTaskType.JDBCSQL
        }, // Free-text, only active if "JDBC SQL" is selected in taskType, varchar(256)
        {
          label: 'Airflow Pool',
          value: row.airflowPool,
          type: SettingType.Readonly
        }, // Free-text, varchar(64)
        {
          label: 'Airflow Priority',
          value: row.airflowPriority,
          type: SettingType.Text
        }, // Free-text, varchar(64)
        {
          label: 'Include In Airflow',
          value: row.includeInAirflow,
          type: SettingType.Boolean
        }, // Boolean true or false, required (default value: true)
        {
          label: 'Task Dependency Downstream',
          value: row.taskDependencyDownstream,
          type: SettingType.Text
        }, // Free-text, Defines the downstream dependency for the Task. Comma separated list, varchar(256)
        {
          label: 'Task Dependency Upstream',
          value: row.taskDependencyUpstream,
          type: SettingType.Text
        }, // Free-text, Defines the upstream dependency for the Task. Comma separated list, varchar(256)
        {
          label: 'Task Config',
          value: row.taskConfig,
          type: SettingType.Text
        }, // Free-text, The configuration for the Task. Depends on what Task type it is,, varchar(512)
        {
          label: 'Sensor Poke Interval',
          value: row.sensorPokeInterval,
          type: SettingType.IntegerFromZeroOrNull
        }, // Number, Poke interval for sensors in seconds, int(11)
        {
          label: 'Sensor Timeout Minutes',
          value: row.sensorTimeoutMinutes,
          type: SettingType.Readonly
        }, // Number, Timeout for sensors in minutes, int(11)
        {
          label: 'Sensor Connection',
          value: row.sensorConnection,
          type: SettingType.Text
        }, // Free-text, Name of Connection in Airflow, varchar(64)
        {
          label: 'Sensor Soft Fail',
          value: row.sensorSoftFail === 1 ? 1 : 0,
          type: SettingType.BooleanNumber
        }, // Boolean number, Setting this to 1 will add soft_fail=True on sensor (1=true, all else = false), int(11)
        {
          label: 'Sudo User',
          value: row.sudoUser,
          type: SettingType.Text
        } // Free-text, The task will use this user for sudo instead of default, varchar(64)
      ]

      setCurrentRow(rowData)
      setModalOpen(true)
    },
    [dagData]
  )

  if (isFetching) return <div>Loading...</div>
  if (!dagData) return <div>No data found.</div>

  const tasksData = dagData.tasks

  const handleSave = (updatedSettings: EditSetting[]) => {
    console.log('updatedSettings', updatedSettings)
    // const editedTableData = updateTableData(dagData, updatedSettings, true)
    // updateTable(editedTableData, {
    //   onSuccess: (response) => {
    //     queryClient.invalidateQueries({ queryKey: ['table', dagName] })
    //     console.log('Update successful', response)
    //     setModalOpen(false)
    //   },
    //   onError: (error) => {
    //     queryClient.invalidateQueries({ queryKey: ['table', dagName] })
    //     console.error('Error updating table', error)
    //   }
    // })
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

      {isModalOpen && currentRow && (
        <EditTableModal
          title={`Edit DAG ${currentRow[0].value}`}
          settings={currentRow}
          onClose={() => setModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default AirflowTasks
