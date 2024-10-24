import { useCallback, useEffect, useRef, useState } from 'react'
import { getAllTimezones } from 'countries-and-timezones'
import Dropdown from '../components/Dropdown'
import { EditSetting } from './interfaces'
import './TableInputFields.scss'

interface TableInputFieldsProps {
  index: number
  setting: EditSetting
  handleInputChange: (
    index: number,
    newValue: string | number | boolean | null
  ) => void
  handleSelect: (
    item: string | number | boolean | null,
    keyLabel?: string
  ) => void
  prevValue?: string | number | boolean
  connectionNames?: string[]
  isCustomQueryDisabled?: boolean
  disabled?: boolean
}

// To do: Add label for and input id on all relevant places

function TableInputFields({
  setting,
  index,
  handleInputChange,
  handleSelect,
  prevValue = '',
  isCustomQueryDisabled = true,
  connectionNames,
  disabled = false
}: TableInputFieldsProps) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  const dropdownId = `dropdown-${index}`
  const isRequired =
    setting.label === 'Database' ||
    setting.label === 'Table' ||
    setting.label === 'Source Schema' ||
    setting.label === 'Source Table' ||
    setting.label === 'Connection String' ||
    setting.label === 'DAG Name'

  const showRequiredIndicator = isRequired && !setting.value

  const autoResizeTextarea = useCallback(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height =
        setting.label === 'Connection String'
          ? `${textareaRef.current.scrollHeight}px`
          : '50px'
      textareaRef.current.style.maxHeight = `${textareaRef.current.scrollHeight}px`
    }
  }, [setting.label])

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  useEffect(() => {
    if (setting.value) {
      autoResizeTextarea()
    }
  }, [setting.value, autoResizeTextarea])

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
            <span className="table-input-fields-changed">Changed</span>
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
              Default from Config
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
              Default from Connection
            </label>
          </div>
        </>
      )

    case 'readonly':
      return (
        <>
          <label htmlFor={`text-input-${index}`}>{setting.label}:</label>
          <span id={`text-input-${index}`}>{setting.value}</span>
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
                isCustomQueryDisabled ? 'table-input-fields-label-disabled' : ''
              }
              htmlFor={`text-input-${index}`}
            >
              {setting.label}:
            </label>
            <input
              className="table-input-fields-text-input"
              id={`text-input-${index}`}
              type="text"
              value={setting.value ? String(setting.value) : ''}
              onChange={(event) => handleInputChange(index, event.target.value)}
              disabled={isCustomQueryDisabled}
            />
          </>
        )
      }
      return (
        <>
          <label htmlFor={`text-input-${index}`}>
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <input
            className="table-input-fields-text-input"
            id={`text-input-${index}`}
            type="text"
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
            required={isRequired}
          />
        </>
      )
    }

    case 'textarea': {
      return (
        <>
          <label htmlFor={`textarea-input-${index}`}>
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <textarea
            id={`textarea-input-${index}`}
            ref={textareaRef}
            value={setting.value ? String(setting.value) : ''}
            style={{
              width: 'calc(100% - 217px)',
              maxWidth: 'calc(100% - 217px)',
              minWidth: 'calc(100% - 217px)'
            }}
            onChange={(event) => {
              handleInputChange(index, event.target.value)
              autoResizeTextarea()
            }}
            required={isRequired}
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
            fontSize="14px"
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
          <label style={disabled ? { color: '#aeaeae' } : {}}>
            {setting.label}:
          </label>

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
            initialTitle={setting.value ? String(setting.value) : 'Select...'}
            cross={true}
            backgroundColor="inherit"
            textColor="black"
            fontSize="14px"
            border="0.5px solid rgb(42, 42, 42)"
            borderRadius="3px"
            height="21.5px"
            padding="8px 3px"
            chevronWidth="11"
            chevronHeight="7"
            lightStyle={true}
            disabled={disabled}
          />
        </>
      )

    case 'connectionReferenceRequired':
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
            fontSize="14px"
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
            className="table-input-fields-number-input"
            type="number"
            value={
              setting.value !== null && setting.value !== undefined
                ? Number(setting.value)
                : ''
            }
            onChange={(event) => {
              let value: string | number | null =
                event.target.value === '' ? '' : Number(event.target.value)

              if (
                isNaN(Number(value)) &&
                typeof value === 'number' &&
                value < 0
              ) {
                value = null
                event.target.value = ''
              }

              handleInputChange(
                index,
                value === '' || isNaN(Number(value)) ? null : value
              )
            }}
            onBlur={(event) => {
              const value = event.target.value
              // If input is empty or not a valid number greater than 1, set to null
              if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
                handleInputChange(index, null)
                event.target.value = ''
              }
            }}
            onKeyDown={(event) => {
              // Prevent invalid characters from being typed
              if (
                ['0', 'e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key) // Dead is still working, fix so it is not
              ) {
                event.preventDefault()
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
            className="table-input-fields-number-input"
            type="number"
            value={
              setting.value !== null && setting.value !== undefined
                ? Number(setting.value)
                : ''
            }
            onChange={(event) => {
              let value: string | number | null =
                event.target.value === '' ? '' : Number(event.target.value)

              if (typeof value === 'number' && value < 0) {
                value = null
              }

              handleInputChange(index, value === '' ? null : value)
            }}
            onBlur={(event) => {
              const value = event.target.value
              // If input is empty or not a valid number greater than 1, set to null
              if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
                handleInputChange(index, null)
                event.target.value = ''
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
              className="table-input-fields-number-input"
              type="number"
              value={
                setting.value === -1
                  ? '' // Shows empty string when disabled (Auto is checked)
                  : setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(event) => {
                let value: string | number =
                  event.target.value === '' ? '' : Number(event.target.value)

                if (typeof value === 'number' && value < 0) {
                  value = -1
                }

                handleInputChange(index, value === '' ? null : value)
              }}
              onBlur={(event) => {
                const value = event.target.value
                // If input is empty or not a valid number greater than 1, set to Auto (-1)
                if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
                  handleInputChange(index, -1)
                  event.target.value = ''
                }
              }}
              step="1"
              disabled={setting.value === -1}
            />
            <label>
              <input
                type="checkbox"
                checked={setting.value === -1}
                onChange={(event) => {
                  if (event.target.checked) {
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
              className="table-input-fields-number-input"
              type="number"
              value={
                setting.value === -1 || setting.value === 0
                  ? '' // Shows empty string when disabled (Auto is checked)
                  : setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(event) => {
                let value: string | number =
                  event.target.value === '' ? '' : Number(event.target.value)

                if (typeof value === 'number' && value < -1) {
                  value = -1
                }

                handleInputChange(index, value === '' ? null : value)
              }}
              onBlur={(event) => {
                const value = event.target.value
                // If input is empty or not a valid number greater than 1, set to Auto (-1)
                if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
                  handleInputChange(index, -1)
                  event.target.value = ''
                }
              }}
              step="1"
              disabled={setting.value === 0 || setting.value === -1}
            />
            <label>
              <input
                type="checkbox"
                checked={setting.value === -1 || setting.value === 0}
                onChange={(event) => {
                  if (event.target.checked) {
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
              className="table-input-fields-number-input"
              type="number"
              value={
                setting.value === null
                  ? '' // Shows empty string when disabled (Default from Config is checked)
                  : setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(event) => {
                let value: string | number | null =
                  event.target.value === '' ? '' : Number(event.target.value)

                if (typeof value === 'number' && value < 1) {
                  value = null
                }

                handleInputChange(index, value === '' ? null : value)
              }}
              onBlur={(event) => {
                const value = event.target.value
                // If input is empty or not a valid number greater than 1, set to null
                if (value === '' || isNaN(Number(value)) || Number(value) < 1) {
                  handleInputChange(index, null)
                  event.target.value = ''
                }
              }}
              step="1"
              disabled={setting.value === null}
            />
            <label>
              <input
                type="checkbox"
                checked={setting.value === null}
                onChange={(event) => {
                  if (event.target.checked) {
                    handleInputChange(index, null) // Sets to null for Default from Config
                  } else {
                    handleInputChange(index, prevValue === '' ? 1 : prevValue) // Restores the previous value
                  }
                }}
              />
              Default from Config
            </label>
          </div>
        </>
      )

    case 'time': {
      return (
        <>
          <label>{setting.label}:</label>
          <input
            className="table-input-fields-text-input"
            type="time"
            step="1"
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
          />
        </>
      )
    }

    case 'timezone': {
      // const userTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone // Maybe use if field is required
      // console.log("User's current time zone:", userTimeZone)
      const allTimeZones = getAllTimezones({ deprecated: true })

      const timeZoneNames = Object.keys(allTimeZones)
      timeZoneNames.sort((a, b) => a.localeCompare(b))

      return (
        <>
          <label>{setting.label}:</label>
          <Dropdown
            keyLabel={setting.label}
            items={timeZoneNames}
            onSelect={handleSelect}
            isOpen={openDropdown === dropdownId}
            onToggle={(isOpen: boolean) =>
              handleDropdownToggle(dropdownId, isOpen)
            }
            searchFilter={true}
            initialTitle={
              setting.value
                ? String(setting.value)
                : // userTimeZone ||
                  'Select...'
            }
            cross={true}
            backgroundColor="inherit"
            textColor="black"
            fontSize="14px"
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

    case 'groupingSpace':
      return <div className="setting-grouping-space"> </div>

    default:
      return null
  }
}

export default TableInputFields
