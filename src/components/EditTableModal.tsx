import { useMemo, useState } from 'react'
import { EditSetting, EditSettingValueTypes } from '../utils/interfaces'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'

interface EditModalProps {
  title: string
  settings: EditSetting[]
  onSave: (newSettings: EditSetting[]) => void
  onClose: () => void
}

function EditTableModal({ title, settings, onSave, onClose }: EditModalProps) {
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

  const validationMethodSetting = editedSettings.find(
    (s) => s.label === 'Validation Method'
  )
  const isCustomQueryDisabled =
    validationMethodSetting?.value !== 'Custom Query'

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredLabels = [
      'Database',
      'Table',
      'Source Schema',
      'Source Table'
    ]
    return editedSettings.some(
      (setting) => requiredLabels.includes(setting.label) && !setting.value
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

  return (
    <div className="table-modal-backdrop">
      <div className="table-modal-content">
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
                <div key={index} className="table-modal-setting">
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    prevValue={prevValue}
                    isCustomQueryDisabled={isCustomQueryDisabled}
                    connectionNames={connectionNames}
                  />
                </div>
              ))}
          </div>
          <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />

          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button type="submit" title="Save" />
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

export default EditTableModal
