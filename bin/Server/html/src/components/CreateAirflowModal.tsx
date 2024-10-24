import { useMemo, useState } from 'react'
import { EditSetting, EditSettingValueTypes } from '../utils/interfaces'
import { useAllAirflows } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'
import { createAirflowSettings } from '../utils/cardRenderFormatting'

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
  const settings = createAirflowSettings(type)

  const { data: airflowsData } = useAllAirflows()
  const airflowNames = useMemo(
    () =>
      Array.isArray(airflowsData)
        ? airflowsData.map((airflow) => airflow.name)
        : [],
    [airflowsData]
  )
  console.log('airflowNames', airflowNames)

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(
    settings ? settings : []
  )
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [duplicateDagName, setDuplicateDagName] = useState(false) // State to track duplicate DAG Name

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredLabels = ['DAG Name']
    return editedSettings.some(
      (setting) => requiredLabels.includes(setting.label) && !setting.value
    )
  }, [editedSettings])

  if (!settings) {
    console.error('DAG data is not available.')
    return
  }

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

export default CreateAirflowModal
