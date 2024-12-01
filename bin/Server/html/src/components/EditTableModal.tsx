import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState
} from 'react'
import { EditSetting, EditSettingValueTypes } from '../utils/interfaces'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'
import InfoText from './InfoText'
import { AirflowDAGTaskType } from '../utils/enums'
import { useLocation } from 'react-router-dom'
import { isValidOctal } from '../utils/functions'

interface EditModalProps {
  title: string
  settings: EditSetting[]
  onSave: (newSettings: EditSetting[]) => void
  onClose: () => void
  initWidth?: number
}

function EditTableModal({
  title,
  settings,
  onSave,
  onClose,
  initWidth
}: EditModalProps) {
  const editableSettings =
    settings?.filter((setting) => {
      const isReadonly = setting.type === 'readonly'
      const isHidden = setting.isHidden
      const isDatabaseOrTable =
        setting.label === 'Database' || setting.label === 'Table'

      return !isHidden && (!isReadonly || isDatabaseOrTable)
    }) ?? []
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const location = useLocation()
  const pathnames = location.pathname.split('/').filter((x) => x)
  const [isNotTripleOctalValue, setIsNotTripleOctalValue] = useState(false)

  const [originalEditableSettings] = useState(editableSettings)
  const [editedSettings, setEditedSettings] = useState(editableSettings)
  const [changedSettings, setChangedSettings] = useState<
    Map<
      string,
      {
        original: EditSettingValueTypes | null
        new: string | number | boolean | null
      }
    >
  >(new Map())

  const [prevValue, setPrevValue] = useState<string | number | boolean>('')
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [modalWidth, setModalWidth] = useState(initWidth ? initWidth : 584)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(initWidth ? initWidth : 584)
  const MIN_WIDTH = initWidth ? initWidth : 584

  const validationMethodSetting = editedSettings.find(
    (s) => s.label === 'Validation Method'
  )

  const isValidationCustomQueryDisabled =
    validationMethodSetting?.value !== 'Custom Query'

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

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredFields = editedSettings.filter(
      (setting) => setting.isRequired
    )

    return requiredFields.some(
      (setting) => setting.value === null || setting.value === ''
    )
  }, [editedSettings])

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null
  ) => {
    // Creates a new array, copying all elements of editedSettings
    const newSettings = [...editedSettings]
    const setting = newSettings[index]
    const currentValue = newSettings[index].value
    const originalValue = originalEditableSettings[index]?.value

    if (setting.label === 'DAG File Permission') {
      const isValueValid = isValidOctal(newValue as string)
      setIsNotTripleOctalValue(!isValueValid)
    }

    if (typeof originalValue === 'string' && pathnames[1] === 'global') {
      setPrevValue(originalValue)
    }

    if (
      (setting.type === 'integerFromOne' ||
        setting.type === 'integerFromZero') &&
      typeof originalValue === 'number' &&
      Number.isInteger(originalValue)
    ) {
      setPrevValue(originalValue)
    }

    // Only stores previous value if it's a whole number
    if (newValue === -1) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== -1 &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }

    // Stores previous value if it's a whole number when newValue is null
    if (newValue === null) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== null &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }

    // Creates a new object for the setting being updated
    const updatedSetting = { ...newSettings[index], value: newValue }

    // Replaces the old object in the array with the new object
    newSettings[index] = updatedSetting
    const updatedChangedSettings = new Map(changedSettings)
    if (newValue !== originalValue) {
      updatedChangedSettings.set(setting.label, {
        original: originalValue,
        new: newValue
      })
    } else {
      updatedChangedSettings.delete(setting.label)
    }

    setChangedSettings(updatedChangedSettings)
    setEditedSettings(newSettings)
  }

  const handleSelect = (
    item: string | number | boolean | null,
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
    if (isNotTripleOctalValue) return

    // Creates a new updatedSettings array by merging editedSettings into the original settings, ensuring immutability.
    const updatedSettings = Array.isArray(settings)
      ? settings.map((setting) => {
          const editedSetting = editedSettings.find(
            (es) => es.label === setting.label
          )

          return editedSetting
            ? { ...setting, ...editedSetting }
            : { ...setting }
        })
      : []
    onSave(updatedSettings)
    onClose()
  }

  const handleCancelClick = () => {
    if (changedSettings.size > 0) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setEditedSettings(originalEditableSettings)
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

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
    [isResizing, initialMouseX, initialWidth, MIN_WIDTH]
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
        <h2 className="table-modal-h2">{title}</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            handleSave()
          }}
        >
          <div className="table-modal-body">
            {Array.isArray(editedSettings) &&
              editedSettings.map((setting, index) => (
                <div key={index} className={'table-modal-setting'}>
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    prevValue={prevValue}
                    connectionNames={connectionNames}
                    isValidationCustomQueryDisabled={
                      isValidationCustomQueryDisabled
                    }
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
                      iconPosition={
                        setting.type === 'readonly'
                          ? { paddingTop: 0, paddingBottom: 2 }
                          : { paddingTop: 2 }
                      }
                      infoTextMaxWidth={
                        setting.type === 'text' ||
                        setting.type === 'textarea' ||
                        setting.type === 'booleanNumberOrAuto' ||
                        setting.type === 'booleanOrDefaultFromConfig(-1)' ||
                        setting.type === 'booleanOrDefaultFromConnection(-1)' ||
                        setting.type ===
                          'integerFromOneOrDefaultFromConfig(null)'
                          ? 430
                          : setting.type === 'enum'
                          ? 280
                          : setting.type === 'boolean' ||
                            setting.type === 'booleanNumber'
                          ? 380
                          : setting.type === 'integerOneOrTwo' ||
                            setting.type === 'integerFromZero' ||
                            setting.type === 'integerFromOne' ||
                            setting.type === 'integerFromZeroOrNull' ||
                            setting.type === 'integerFromOneOrNull' ||
                            setting.type === 'integerFromZeroOrAuto(-1)' ||
                            setting.type === 'integerFromOneOrAuto(-1)'
                          ? 343
                          : 270
                      }
                      isInfoTextPositionUp={
                        setting.label === 'Spark Executors' ||
                        setting.label === 'Spark Max Executors'
                          ? true
                          : false
                      }
                    />
                  )}
                </div>
              ))}
          </div>
          {location.pathname === '/configuration/global' ? (
            <RequiredFieldsInfo
              isRequiredFieldEmpty={isRequiredFieldEmpty}
              validation={true}
              isValidationSad={isNotTripleOctalValue}
              validationText="DAG File Permission must be a 3-digit or 4-digit octal value."
            />
          ) : (
            <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />
          )}

          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Save"
              disabled={isRequiredFieldEmpty}
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Edit"
          message="Any unsaved changes will be lost."
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Cancel"
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          isActive={showConfirmation}
        />
      )}
    </div>
  )
}

export default EditTableModal
