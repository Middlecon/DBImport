import { useMemo, useState } from 'react'
import { EditSetting, EditSettingValueTypes } from '../utils/interfaces'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'
import { initialCreateImportTableSettings } from '../utils/cardRenderFormatting'

interface CreateTableModalProps {
  database: string
  prefilledConnection: string
  onSave: (newTableData: EditSetting[]) => void
  onClose: () => void
}

function CreateTableModal({
  database,
  prefilledConnection,
  onSave,
  onClose
}: CreateTableModalProps) {
  const settings = initialCreateImportTableSettings(
    database,
    prefilledConnection
  )
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(settings)
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

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
    newValue: EditSettingValueTypes | null
  ) => {
    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

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
    const newTableSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    onSave(newTableSettings)
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
      <div className="table-modal-content">
        <h2 className="table-modal-h2">Create table</h2>
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

export default CreateTableModal
