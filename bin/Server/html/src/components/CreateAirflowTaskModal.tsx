import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState
} from 'react'
import {
  AirflowTask,
  EditSetting,
  EditSettingValueTypes
} from '../utils/interfaces'
// import { useAllAirflows } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'
import { initialCreateAirflowTaskSettings } from '../utils/cardRenderFormatting'
import InfoText from './InfoText'
import { AirflowDAGTaskType } from '../utils/enums'
import { useConnections } from '../utils/queries'

interface CreateAirflowModalProps {
  type: 'import' | 'export' | 'custom'
  tasksData: AirflowTask[]
  onSave: (newTableData: EditSetting[]) => void
  onClose: () => void
}

function CreateAirflowTaskModal({
  type,
  tasksData,
  onSave,
  onClose
}: CreateAirflowModalProps) {
  // const { data: airflowsData } = useAllAirflows()

  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const settings = initialCreateAirflowTaskSettings()

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(
    settings ? settings : []
  )

  const airflowNames = useMemo(
    () => (Array.isArray(tasksData) ? tasksData.map((task) => task.name) : []),
    [tasksData]
  )
  const [duplicateTaskName, setDuplicateTaskName] = useState(false)

  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [pendingValidation, setPendingValidation] = useState(false)

  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredLabels = ['Task Name']
    return editedSettings.some(
      (setting) =>
        (requiredLabels.includes(setting.label) && setting.value === null) ||
        setting.value === ''
    )
  }, [editedSettings])

  const MIN_WIDTH = 584

  const handleMouseDown = (e: { clientX: SetStateAction<number> }) => {
    setIsResizing(true)
    setInitialMouseX(e.clientX)
    setInitialWidth(modalWidth)
  }

  const handleMouseUp = useCallback(() => {
    setIsResizing(false)
  }, [])

  const handleMouseMove = useCallback(
    (e: { clientX: number }) => {
      if (isResizing) {
        const deltaX = e.clientX - initialMouseX
        setModalWidth(Math.max(initialWidth + deltaX, MIN_WIDTH))
      }
    },
    [isResizing, initialMouseX, initialWidth]
  )

  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    } else {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing, handleMouseMove, handleMouseUp])

  if (!settings) {
    console.error('Task data is not available.')
    return
  }

  const handleInputChange = (
    index: number,
    newValue: EditSettingValueTypes | null
  ) => {
    setPendingValidation(true)

    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

    const taskNameSetting = updatedSettings.find(
      (setting) => setting.label === 'Task Name'
    )

    if (
      taskNameSetting &&
      airflowNames.includes(taskNameSetting.value as string)
    ) {
      setDuplicateTaskName(true)
    } else {
      setDuplicateTaskName(false)
      setPendingValidation(false)
    }

    setEditedSettings(updatedSettings)
    setHasChanges(true)
  }

  const airflowTypeValue = editedSettings.find((s) => s.label === 'Type')

  const isAirflowTasksConnectionDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.JDBCSQL

  const isAirflowTasksSensorPokeAndSoftDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.DAGSensor &&
    airflowTypeValue?.value !== AirflowDAGTaskType.SQLSensor

  const isAirflowTasksSensorConnectionDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.SQLSensor

  const isAirflowTasksSudoUserDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.DBImportCommand &&
    airflowTypeValue?.value !== AirflowDAGTaskType.JDBCSQL &&
    airflowTypeValue?.value !== AirflowDAGTaskType.HiveSQL &&
    airflowTypeValue?.value !== AirflowDAGTaskType.HiveSQLScript

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string
  ) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleSave = () => {
    const newTaskSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    onSave(newTaskSettings)
    onClose()
  }

  const handleCancelClick = () => {
    if (hasChanges) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  return (
    <div className="table-modal-backdrop">
      <div className="table-modal-content" style={{ width: `${modalWidth}px` }}>
        <div
          className="table-modal-resize-handle left"
          onMouseDown={handleMouseDown}
        ></div>
        <div
          className="table-modal-resize-handle right"
          onMouseDown={handleMouseDown}
        ></div>
        <h2 className="table-modal-h2">{`Create ${
          type.charAt(0).toUpperCase() + type.slice(1)
        } DAG task`}</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            handleSave()
          }}
        >
          <div className="table-modal-body">
            {editedSettings &&
              editedSettings.map((setting, index) => (
                <div key={index} className="table-modal-setting">
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    connectionNames={connectionNames}
                    isAirflowTasksSensorPokeAndSoftDisabled={
                      isAirflowTasksSensorPokeAndSoftDisabled
                    }
                    isAirflowTasksSensorConnectionDisabled={
                      isAirflowTasksSensorConnectionDisabled
                    }
                    isAirflowTasksSudoUserDisabled={
                      isAirflowTasksSudoUserDisabled
                    }
                    disabled={isAirflowTasksConnectionDisabled}
                  />
                  {setting.infoText && setting.infoText.length > 0 && (
                    <InfoText
                      label={setting.label}
                      infoText={setting.infoText}
                    />
                  )}
                </div>
              ))}
          </div>
          <RequiredFieldsInfo
            isRequiredFieldEmpty={isRequiredFieldEmpty}
            validation={true}
            isValidationSad={duplicateTaskName}
            validationText="Task Name already exists on this DAG. Please choose a different name."
          />

          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Save"
              disabled={
                isRequiredFieldEmpty || duplicateTaskName || pendingValidation
              }
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          message="Any unsaved changes will be lost."
        />
      )}
    </div>
  )
}

export default CreateAirflowTaskModal
