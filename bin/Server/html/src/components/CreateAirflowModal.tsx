import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useState
} from 'react'
import { EditSetting, EditSettingValueTypes } from '../utils/interfaces'
import { useAllAirflows } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'
import { initialCreateAirflowSettings } from '../utils/cardRenderFormatting'
import InfoText from './InfoText'

interface CreateAirflowModalProps {
  type: 'import' | 'export' | 'custom'
  onSave: (newTableData: EditSetting[]) => void
  onClose: () => void
}

function CreateAirflowModal({
  type,
  onSave,
  onClose
}: CreateAirflowModalProps) {
  const settings = initialCreateAirflowSettings(type)

  const { data: airflowsData } = useAllAirflows()
  const airflowNames = useMemo(
    () =>
      Array.isArray(airflowsData)
        ? airflowsData.map((airflow) => airflow.name)
        : [],
    [airflowsData]
  )

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(
    settings ? settings : []
  )
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

  // For validating unique pk so it becomes a create and not an update
  const [duplicateDagName, setDuplicateDagName] = useState(false)
  const [pendingValidation, setPendingValidation] = useState(false)

  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredLabels = ['DAG Name']
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
    console.error('DAG data is not available.')
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

    const dagNameSetting = updatedSettings.find(
      (setting) => setting.label === 'DAG Name'
    )
    if (
      dagNameSetting &&
      airflowNames.includes(dagNameSetting.value as string)
    ) {
      setDuplicateDagName(true)
    } else {
      setDuplicateDagName(false)
      setPendingValidation(false)
    }

    setEditedSettings(updatedSettings)
    setHasChanges(true)
  }

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
    if (duplicateDagName) return

    const newDagSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    onSave(newDagSettings)
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
        <h2 className="table-modal-h2">Create DAG</h2>
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
            isValidationSad={duplicateDagName}
            validationText="DAG Name already exists. Please choose a different name."
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
                isRequiredFieldEmpty || duplicateDagName || pendingValidation
              }
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Create DAG"
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

export default CreateAirflowModal
