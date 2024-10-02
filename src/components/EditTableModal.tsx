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
  const editableSettings = settings.filter((setting) => {
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

  const [originalSettings] = useState(editableSettings)
  const [editedSettings, setEditedSettings] = useState(editableSettings)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [prevValue, setPrevValue] = useState<string | number | boolean>('')
  const [showConfirmation, setShowConfirmation] = useState(false)

  const validationMethodSetting = editedSettings.find(
    (s) => s.label === 'Validation Method'
  )
  const isCustomQueryDisabled =
    validationMethodSetting?.value !== 'Custom Query'

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null
  ) => {
    const newSettings = [...editedSettings]
    const currentValue = newSettings[index].value

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

    // Sets the new value
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

      return editedSetting ? editedSetting : setting
    })

    onSave(updatedSettings)
    onClose()
  }

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
    const dropdownId = `dropdown-${index}`

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
      case 'booleanNumber':
        return (
          <>
            <label>{setting.label}:</label>
            <div className="radio-edit">
              <label>
                <input
                  type="radio"
                  name={`booleanNumber-${index}`}
                  value="true"
                  checked={setting.value === 1}
                  onChange={() => handleInputChange(index, 1)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanNumber-${index}`}
                  value="false"
                  checked={setting.value === 0}
                  onChange={() => handleInputChange(index, 0)}
                />
                False
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
                  checked={setting.value === 1}
                  onChange={() => handleInputChange(index, 1)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConfig(-1)-${index}`}
                  value="false"
                  checked={setting.value === 0}
                  onChange={() => handleInputChange(index, 0)}
                />
                False
              </label>
              <label className="label-default-from-config">
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConfig(-1)-${index}`}
                  value="-1"
                  checked={setting.value === -1}
                  onChange={() => handleInputChange(index, -1)}
                />
                Default from config
              </label>
            </div>
          </>
        )

      case 'booleanOrDefaultFromConnection(-1)':
        return (
          <>
            <label>{setting.label}:</label>
            <div className="radio-edit">
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConnection(-1)-${index}`}
                  value="true"
                  checked={setting.value === true}
                  onChange={() => handleInputChange(index, 1)}
                />
                True
              </label>
              <label>
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConnection(-1)-${index}`}
                  value="false"
                  checked={setting.value === false}
                  onChange={() => handleInputChange(index, 0)}
                />
                False
              </label>
              <label className="label-default-from-config">
                <input
                  type="radio"
                  name={`booleanOrDefaultFromConnection(-1)-${index}`}
                  value="-1"
                  checked={setting.value === -1}
                  onChange={() => handleInputChange(index, -1)}
                />
                Default from connection
              </label>
            </div>
          </>
        )

      case 'readonly':
        return (
          <>
            <label>{setting.label}:</label>
            <span>{setting.value}</span>
          </>
        )

      case 'text': {
        if (
          setting.label === 'Custom Query Source SQL' ||
          setting.label === 'Custom Query Hive SQL'
        ) {
          return (
            <>
              <label
                className={
                  isCustomQueryDisabled ? 'edit-table-modal-label-disabled' : ''
                }
              >
                {setting.label}:
              </label>
              <input
                className="edit-table-modal-text-input"
                type="text"
                value={setting.value ? String(setting.value) : ''}
                onChange={(e) => handleInputChange(index, e.target.value)}
                disabled={isCustomQueryDisabled} // Conditionally disable the input
              />
            </>
          )
        }
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
      }

      case 'enum': {
        const dropdownOptions = setting.enumOptions
          ? Object.values(setting.enumOptions)
          : []

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
      }

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

      case 'integerFromOneOrNull':
        return (
          <>
            <label>{setting.label}:</label>
            <input
              className="edit-table-modal-number-input"
              type="number"
              value={
                setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(e) => {
                let value: string | number | null =
                  e.target.value === '' ? '' : Number(e.target.value)

                if (
                  isNaN(Number(value)) &&
                  typeof value === 'number' &&
                  value < 0
                ) {
                  value = null
                  e.target.value = ''
                }

                handleInputChange(
                  index,
                  value === '' || isNaN(Number(value)) ? null : value
                )
              }}
              onBlur={(e) => {
                const value = e.target.value
                // If input is empty or not a valid number greater than 1, set to null
                if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
                  handleInputChange(index, null)
                  e.target.value = ''
                }
              }}
              onKeyDown={(e) => {
                // Prevent invalid characters from being typed
                if (
                  ['0', 'e', 'E', '+', '-', '.', ',', 'Dead'].includes(e.key) // Dead is still working, fix so it is not
                ) {
                  e.preventDefault()
                }
              }}
              step="1"
            />
          </>
        )

      case 'integerFromZeroOrNull':
        return (
          <>
            <label>{setting.label}:</label>
            <input
              className="edit-table-modal-number-input"
              type="number"
              value={
                setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(e) => {
                let value: string | number | null =
                  e.target.value === '' ? '' : Number(e.target.value)

                if (typeof value === 'number' && value < 0) {
                  value = null
                }

                handleInputChange(index, value === '' ? null : value)
              }}
              onBlur={(e) => {
                const value = e.target.value
                // If input is empty or not a valid number greater than 1, set to null
                if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
                  handleInputChange(index, null)
                  e.target.value = ''
                }
              }}
              step="1"
            />
          </>
        )

      case 'integerFromZeroOrAuto(-1)':
        return (
          <>
            <label>{setting.label}:</label>

            <div>
              <input
                className="edit-table-modal-number-input"
                type="number"
                value={
                  setting.value === -1
                    ? '' // Shows empty string when disabled (Auto is checked)
                    : setting.value !== null && setting.value !== undefined
                    ? Number(setting.value)
                    : ''
                }
                onChange={(e) => {
                  let value: string | number =
                    e.target.value === '' ? '' : Number(e.target.value)

                  if (typeof value === 'number' && value < 0) {
                    value = -1
                  }

                  handleInputChange(index, value === '' ? null : value)
                }}
                onBlur={(e) => {
                  const value = e.target.value
                  // If input is empty or not a valid number greater than 1, set to Auto (-1)
                  if (
                    value === '' ||
                    isNaN(Number(value)) ||
                    Number(value) < 0
                  ) {
                    handleInputChange(index, -1)
                    e.target.value = ''
                  }
                }}
                step="1"
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
                      handleInputChange(index, prevValue === '' ? 0 : prevValue) // Restores the previous value
                    }
                  }}
                />
                Auto
              </label>
            </div>
          </>
        )

      case 'integerFromOneOrAuto(-1)':
        return (
          <>
            <label>{setting.label}:</label>

            <div>
              <input
                className="edit-table-modal-number-input"
                type="number"
                value={
                  setting.value === -1 || setting.value === 0
                    ? '' // Shows empty string when disabled (Auto is checked)
                    : setting.value !== null && setting.value !== undefined
                    ? Number(setting.value)
                    : ''
                }
                onChange={(e) => {
                  let value: string | number =
                    e.target.value === '' ? '' : Number(e.target.value)

                  if (typeof value === 'number' && value < -1) {
                    value = -1
                  }

                  handleInputChange(index, value === '' ? null : value)
                }}
                onBlur={(e) => {
                  const value = e.target.value
                  // If input is empty or not a valid number greater than 1, set to Auto (-1)
                  if (
                    value === '' ||
                    isNaN(Number(value)) ||
                    Number(value) < 1
                  ) {
                    handleInputChange(index, -1)
                    e.target.value = ''
                  }
                }}
                step="1"
                disabled={setting.value === 0 || setting.value === -1}
              />
              <label>
                <input
                  type="checkbox"
                  checked={setting.value === -1 || setting.value === 0}
                  onChange={(e) => {
                    if (e.target.checked) {
                      handleInputChange(index, -1) // Sets to -1 for Auto
                    } else {
                      handleInputChange(index, prevValue === '' ? 1 : prevValue) // Restores the previous value
                    }
                  }}
                />
                Auto
              </label>
            </div>
          </>
        )

      case 'integerFromOneOrDefaultFromConfig(null)':
        return (
          <>
            <label>{setting.label}:</label>

            <div>
              <input
                className="edit-table-modal-number-input"
                type="number"
                value={
                  setting.value === null
                    ? '' // Shows empty string when disabled (Default from config is checked)
                    : setting.value !== null && setting.value !== undefined
                    ? Number(setting.value)
                    : ''
                }
                onChange={(e) => {
                  let value: string | number | null =
                    e.target.value === '' ? '' : Number(e.target.value)

                  if (typeof value === 'number' && value < 1) {
                    value = null
                  }

                  handleInputChange(index, value === '' ? null : value)
                }}
                onBlur={(e) => {
                  const value = e.target.value
                  // If input is empty or not a valid number greater than 1, set to null
                  if (
                    value === '' ||
                    isNaN(Number(value)) ||
                    Number(value) < 1
                  ) {
                    handleInputChange(index, null)
                    e.target.value = ''
                  }
                }}
                step="1"
                disabled={setting.value === null}
              />
              <label>
                <input
                  type="checkbox"
                  checked={setting.value === null}
                  onChange={(e) => {
                    if (e.target.checked) {
                      handleInputChange(index, null) // Sets to null for Default from config
                    } else {
                      handleInputChange(index, prevValue === '' ? 1 : prevValue) // Restores the previous value
                    }
                  }}
                />
                Default from config
              </label>
            </div>
          </>
        )
      case 'groupingSpace':
        return <div className="setting-grouping-space"> </div>

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
