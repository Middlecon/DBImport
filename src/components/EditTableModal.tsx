import { useMemo, useState } from 'react'
import './EditTableModal.scss'
import { TableSetting } from '../utils/interfaces'
import Dropdown from './Dropdown'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'

interface EditModalProps {
  title: string
  settings: TableSetting[]
  onSave: (newSettings: TableSetting[]) => void
  onClose: () => void
}

function EditTableModal({ title, settings, onSave, onClose }: EditModalProps) {
  const filteredSettings = settings.filter((setting) => {
    const isReadonly = setting.type === 'readonly'
    const isHidden = setting.isHidden
    const isDatabaseOrTable =
      setting.label === 'Database' || setting.label === 'Table'

    return !isHidden && (!isReadonly || isDatabaseOrTable)
  })
  const { data: connectionsData } = useConnections()
  const connectionNames = useMemo(
    () => connectionsData?.map((connection) => connection.name) ?? [],
    [connectionsData]
  )

  console.log('filteredSettings', filteredSettings)

  const [originalSettings] = useState(filteredSettings)
  const [editedSettings, setEditedSettings] = useState(filteredSettings)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [prevValue, setPrevValue] = useState<string | number | boolean>('')
  // const [isChanged, setIsChanged] = useState<boolean>(false)

  console.log('originalSettings', originalSettings)
  // console.log('editedSettings', editedSettings)

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null
  ) => {
    const newSettings = [...editedSettings]
    if (newValue === -1) {
      const currentValue = newSettings[index].value
      if (typeof currentValue === 'number' && currentValue !== -1) {
        setPrevValue(currentValue)
      }
    }

    // if (editedSettings[index].value !== originalSettings[index].value) {
    //   setIsChanged(true)
    // }

    newSettings[index].value = newValue
    setEditedSettings(newSettings)
  }

  const handleSelect = (item: string | number | boolean, keyLabel?: string) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleSave = () => {
    const updatedSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      // Updates the integer type field to 0 if it's null
      if (
        editedSetting &&
        editedSetting.type === 'integer' &&
        editedSetting.value === null
      ) {
        editedSetting.value = 0
      }

      return editedSetting ? editedSetting : setting
    })

    onSave(updatedSettings)
    onClose()
  }

  const [showConfirmation, setShowConfirmation] = useState(false)

  const handleCancelClick = () => {
    setShowConfirmation(true)
  }

  const handleConfirmCancel = () => {
    setEditedSettings(originalSettings)
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  const renderEditSetting = (setting: TableSetting, index: number) => {
    // const isChanged =
    //   editedSettings[index].value !== originalSettings[index].value

    const dropdownId = `dropdown-${index}`
    let dropdownOptions: string[] = []
    if (setting.type === 'enum' && setting.enumOptions) {
      dropdownOptions = Object.values(setting.enumOptions)
    }

    switch (setting.type) {
      case 'boolean':
        return (
          <>
            <label>{setting.label}:</label>
            <div className="radio-edit">
              <label>
                <input
                  type="radio"
                  name={`boolean-${index}`}
                  value="true"
                  checked={setting.value === true}
                  onChange={() => handleInputChange(index, true)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`boolean-${index}`}
                  value="false"
                  checked={setting.value === false}
                  onChange={() => handleInputChange(index, false)}
                />
                False
              </label>
            </div>
            {/* {isChanged && (
              <span className="edit-table-modal-changed">Changed</span>
            )} */}
          </>
        )

      case 'readonly':
        return (
          <>
            <label>{setting.label}:</label>
            <span>{setting.value}</span>
          </>
        )

      case 'text':
        return (
          <>
            <label>{setting.label}:</label>
            <input
              className="edit-table-modal-text-input"
              type="text"
              value={setting.value ? String(setting.value) : ''}
              onChange={(e) => handleInputChange(index, e.target.value)}
            />
          </>
        )

      case 'enum':
        return (
          <>
            <label>{setting.label}:</label>
            <Dropdown
              keyLabel={setting.label}
              items={dropdownOptions}
              onSelect={handleSelect}
              isOpen={openDropdown === dropdownId}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle(dropdownId, isOpen)
              }
              searchFilter={false}
              initialTitle={String(setting.value)}
              backgroundColor="inherit"
              textColor="black"
              border="0.5px solid rgb(42, 42, 42)"
              borderRadius="3px"
              height="21.5px"
              padding="8px 3px"
              chevronWidth="11"
              chevronHeight="7"
              lightStyle={true}
            />
          </>
        )

      case 'integer':
        return (
          <>
            <label>{setting.label}:</label>
            <input
              type="number"
              value={
                setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(e) => {
                const value = e.target.value
                const newValue = value === '' ? null : Number(value)
                handleInputChange(index, newValue)
              }}
              step="1"
              placeholder="0"
            />
          </>
        )
      case 'reference':
        return (
          <>
            <label>{setting.label}:</label>

            <Dropdown
              keyLabel={setting.label}
              items={
                connectionNames.length > 0 ? connectionNames : ['Loading...']
              }
              onSelect={handleSelect}
              isOpen={openDropdown === dropdownId}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle(dropdownId, isOpen)
              }
              searchFilter={true}
              initialTitle={String(setting.value)}
              backgroundColor="inherit"
              textColor="black"
              border="0.5px solid rgb(42, 42, 42)"
              borderRadius="3px"
              height="21.5px"
              padding="8px 3px"
              chevronWidth="11"
              chevronHeight="7"
              lightStyle={true}
            />
          </>
        )
      case 'hidden':
        return null
      case 'booleanOrAuto(-1)':
        return (
          <>
            <label>{setting.label}:</label>
            <div className="radio-edit">
              <label>
                <input
                  type="radio"
                  name={`booleanOrAuto-${index}`}
                  value="true"
                  checked={setting.value === true}
                  onChange={() => handleInputChange(index, true)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrAuto-${index}`}
                  value="false"
                  checked={setting.value === false}
                  onChange={() => handleInputChange(index, false)}
                />
                False
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrAuto-${index}`}
                  value="-1"
                  checked={setting.value === -1}
                  onChange={() => handleInputChange(index, false)}
                />
                Auto
              </label>
            </div>
          </>
        )
      case 'integerOrAuto(-1)':
        return (
          <>
            <label>{setting.label}:</label>

            <div className="radio-edit">
              <input
                type="number"
                value={
                  setting.value === -1
                    ? '' // Shows empty string when disabled (Auto is checked)
                    : setting.value !== null && setting.value !== undefined
                    ? Number(setting.value)
                    : ''
                }
                onChange={(e) => {
                  const value = e.target.value
                  const newValue = value === '' ? null : Number(value)
                  handleInputChange(index, newValue)
                }}
                step="1"
                placeholder={setting.value === -1 ? '' : '0'}
                disabled={setting.value === -1}
              />
              <label>
                <input
                  type="checkbox"
                  checked={setting.value === -1}
                  onChange={(e) => {
                    if (e.target.checked) {
                      handleInputChange(index, -1) // Sets to -1 for Auto
                    } else {
                      handleInputChange(index, prevValue) // Restores the previous value
                    }
                  }}
                />
                Auto
              </label>
            </div>
          </>
        )

      case 'booleanOrDefaultFromConfig(-1)':
        return (
          <>
            <label>{setting.label}:</label>
            <div className="radio-edit">
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConfig(-1)-${index}`}
                  value="true"
                  checked={setting.value === true}
                  onChange={() => handleInputChange(index, true)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConfig(-1)-${index}`}
                  value="false"
                  checked={setting.value === false}
                  onChange={() => handleInputChange(index, false)}
                />
                False
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConfig(-1)-${index}`}
                  value="-1"
                  checked={setting.value === -1}
                  onChange={() => handleInputChange(index, false)}
                />
                Auto
              </label>
            </div>
          </>
        )

      default:
        return null
    }
  }

  return (
    <div className="edit-table-modal-backdrop">
      <div className="edit-table-modal-content">
        <h2 className="edit-table-modal-h2">{title}</h2>
        <form
          onSubmit={(e) => {
            e.preventDefault()
            handleSave()
          }}
        >
          <div className="edit-table-modal-body">
            {editedSettings.map((setting, index) => (
              <div key={index} className="edit-table-modal-setting">
                {renderEditSetting(setting, index)}
              </div>
            ))}
          </div>
          <div className="edit-table-modal-footer">
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
