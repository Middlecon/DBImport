import { useState } from 'react'
import Dropdown from '../components/Dropdown'
import { TableSetting } from './interfaces'

interface TableInputFieldsProps {
  index: number
  setting: TableSetting
  handleInputChange: (
    index: number,
    newValue: string | number | boolean | null
  ) => void
  handleSelect: (item: string | number | boolean, keyLabel?: string) => void
  prevValue: string | number | boolean
  connectionNames?: 'string'[]
  isCustomQueryDisabled?: boolean
}

function TableInputFields({
  setting,
  index,
  handleInputChange,
  handleSelect,
  prevValue,
  isCustomQueryDisabled = true,
  connectionNames
}: TableInputFieldsProps) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const dropdownId = `dropdown-${index}`

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
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

    case 'connectionReference':
      return (
        <>
          <label>{setting.label}:</label>

          <Dropdown
            keyLabel={setting.label}
            items={
              connectionNames && connectionNames.length > 0
                ? connectionNames
                : ['Loading...']
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
                if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
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
                if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
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
                if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
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

export default TableInputFields
