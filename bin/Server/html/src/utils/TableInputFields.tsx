import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getAllTimezones } from 'countries-and-timezones'
import Dropdown from '../components/Dropdown'
import { EditSetting } from './interfaces'
import './TableInputFields.scss'
import { useAtom } from 'jotai'
import { airflowTypeAtom } from '../atoms/atoms'
import { validateEmails } from './functions'

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
  isAirflowEmailDisabled?: boolean
  disabled?: boolean
}

// To do: Add label for and input id on all relevant places

function TableInputFields({
  setting,
  index,
  handleInputChange,
  handleSelect,
  prevValue = '',
  connectionNames,
  isCustomQueryDisabled = true,
  isAirflowEmailDisabled = true,
  disabled = false
}: TableInputFieldsProps) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)
  const [airflowType] = useAtom(airflowTypeAtom)
  const integerFromZeroDefaultValue = useMemo(() => {
    if (
      (setting.label === 'Retries' && airflowType === 'import') ||
      (setting.label === 'Retries' && airflowType === 'export')
    ) {
      return 5
    } else {
      return 0
    }
  }, [setting.label, airflowType])

  const dropdownId = `dropdown-${index}`
  const isRequired =
    setting.label === 'Database' ||
    setting.label === 'Table' ||
    setting.label === 'Source Schema' ||
    setting.label === 'Source Table' ||
    setting.label === 'Connection String' ||
    setting.label === 'DAG Name'

  const showRequiredIndicator = isRequired && !setting.value

  const isFieldDisabled = setting.isConditionsMet === false

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
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div className="radio-edit">
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`boolean-${index}`}
                value="true"
                checked={setting.value === true}
                onChange={() => handleInputChange(index, true)}
                disabled={isFieldDisabled}
              />
              True
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`boolean-${index}`}
                value="false"
                checked={setting.value === false}
                onChange={() => handleInputChange(index, false)}
                disabled={isFieldDisabled}
              />
              False
            </label>
          </div>
          {/* {isChanged && (
            <span className="input-fields-changed">Changed</span>
          )} */}
        </>
      )
    case 'booleanNumber':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div className="radio-edit">
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanNumber-${index}`}
                value="true"
                checked={setting.value === 1}
                onChange={() => handleInputChange(index, 1)}
                disabled={isFieldDisabled}
              />
              True
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanNumber-${index}`}
                value="false"
                checked={setting.value === 0}
                onChange={() => handleInputChange(index, 0)}
                disabled={isFieldDisabled}
              />
              False
            </label>
          </div>
        </>
      )

    case 'booleanNumberOrAuto':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div className="radio-edit">
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanNumber-${index}`}
                value="true"
                checked={setting.value === 1}
                onChange={() => handleInputChange(index, 1)}
                disabled={isFieldDisabled}
              />
              True
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanNumber-${index}`}
                value="false"
                checked={setting.value === 0}
                onChange={() => handleInputChange(index, 0)}
                disabled={isFieldDisabled}
              />
              False
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="checkbox"
                checked={setting.value === -1}
                onChange={(event) => {
                  if (event.target.checked) {
                    handleInputChange(index, -1) // Sets to -1 for Auto
                  } else {
                    handleInputChange(index, prevValue === '' ? -1 : prevValue) // Restores the previous value
                  }
                }}
                disabled={isFieldDisabled}
              />
              Auto
            </label>
          </div>
        </>
      )

    case 'booleanOrDefaultFromConfig(-1)':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div className="radio-edit">
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConfig(-1)-${index}`}
                value="true"
                checked={setting.value === 1}
                onChange={() => handleInputChange(index, 1)}
                disabled={isFieldDisabled}
              />
              True
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConfig(-1)-${index}`}
                value="false"
                checked={setting.value === 0}
                onChange={() => handleInputChange(index, 0)}
                disabled={isFieldDisabled}
              />
              False
            </label>
            <label
              className={
                isFieldDisabled
                  ? ' label-default-from-config input-fields-label-disabled'
                  : 'label-default-from-config'
              }
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConfig(-1)-${index}`}
                value="-1"
                checked={setting.value === -1}
                onChange={() => handleInputChange(index, -1)}
                disabled={isFieldDisabled}
              />
              Default from Config
            </label>
          </div>
        </>
      )

    case 'booleanOrDefaultFromConnection(-1)':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div className="radio-edit">
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConnection(-1)-${index}`}
                value="true"
                checked={setting.value === true}
                onChange={() => handleInputChange(index, 1)}
                disabled={isFieldDisabled}
              />
              True
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConnection(-1)-${index}`}
                value="false"
                checked={setting.value === false}
                onChange={() => handleInputChange(index, 0)}
                disabled={isFieldDisabled}
              />
              False
            </label>
            <label
              className={
                isFieldDisabled
                  ? ' label-default-from-config input-fields-label-disabled'
                  : 'label-default-from-config'
              }
            >
              <input
                type="radio"
                name={`booleanOrDefaultFromConnection(-1)-${index}`}
                value="-1"
                checked={setting.value === -1}
                onChange={() => handleInputChange(index, -1)}
                disabled={isFieldDisabled}
              />
              Default from Connection
            </label>
          </div>
        </>
      )

    case 'readonly':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            htmlFor={`text-input-${index}`}
          >
            {setting.label}:
          </label>
          <span id={`text-input-${index}`}>{setting.value}</span>
        </>
      )

    case 'text': {
      if (
        setting.label === 'Custom Query Source SQL' ||
        setting.label === 'Custom Query Hive SQL' ||
        setting.label === 'Custom Query Target SQL' ||
        setting.label === 'Custom Query' ||
        setting.label === 'Custom Max Query'
      ) {
        return (
          <>
            <label
              className={
                isCustomQueryDisabled ? 'input-fields-label-disabled' : ''
              }
              htmlFor={`text-input-${index}`}
            >
              {setting.label}:
            </label>
            <input
              className="input-fields-text-input"
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
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            htmlFor={`text-input-${index}`}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <input
            className="input-fields-text-input"
            id={`text-input-${index}`}
            type="text"
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
            required={isRequired}
            disabled={isFieldDisabled}
          />
        </>
      )
    }

    case 'textarea': {
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            htmlFor={`textarea-input-${index}`}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <textarea
            id={`textarea-input-${index}`}
            ref={textareaRef}
            value={setting.value ? String(setting.value) : ''}
            className="input-fields-textarea"
            onChange={(event) => {
              handleInputChange(index, event.target.value)
              autoResizeTextarea()
            }}
            required={isRequired}
            disabled={isFieldDisabled}
          />
        </>
      )
    }

    case 'email':
      return (
        <>
          <label
            className={
              isAirflowEmailDisabled ? 'input-fields-label-disabled' : ''
            }
            htmlFor={`email-input-${index}`}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <input
            className="input-fields-text-input"
            id={`email-input-${index}`}
            type="email"
            pattern="^([\w.%+\-]+@[\-a-zA-Z0-9.\-]+\.[\-a-zA-Z]{2,})(, *[\w.%+\-]+@[\-a-zA-Z0-9.\-]+\.[\-a-zA-Z]{2,})*$"
            multiple={true}
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
            onInput={(event) =>
              validateEmails(event.target as HTMLInputElement)
            }
            required={isRequired}
            disabled={isAirflowEmailDisabled}
          />
        </>
      )

    case 'enum': {
      const dropdownOptions = setting.enumOptions
        ? Object.values(setting.enumOptions)
        : []

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
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
          <label
            className={disabled ? 'input-fields-label-disabled' : ''}
            style={disabled ? { color: '#aeaeae' } : {}}
          >
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
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
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

    case 'integerFromZero':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <input
            className="input-fields-number-input"
            type="number"
            value={
              setting.value !== null && setting.value !== undefined
                ? Number(setting.value)
                : integerFromZeroDefaultValue
            }
            onChange={(event) => {
              let value: string | number | null =
                event.target.value === '' ? '' : Number(event.target.value)
              if (typeof value === 'number' && value < 0) {
                value = integerFromZeroDefaultValue
              }

              handleInputChange(index, value === '' ? null : value)
            }}
            onBlur={(event) => {
              const value = event.target.value
              // If input is empty or not a valid number greater than 1, set to default 5
              if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
                handleInputChange(index, integerFromZeroDefaultValue)
                event.target.value = ''
              }
            }}
            onKeyDown={(event) => {
              // Prevent invalid characters from being typed
              if (
                ['e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key) // Dead is still working, fix so it is not
              ) {
                event.preventDefault()
              }
            }}
            step="1"
            disabled={isFieldDisabled}
          />
        </>
      )

    case 'integerFromOneOrNull':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <input
            className="input-fields-number-input"
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
                ['e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key) // Dead is still working, fix so it is not
              ) {
                event.preventDefault()
              }
            }}
            step="1"
            disabled={isFieldDisabled}
          />
        </>
      )

    case 'integerFromZeroOrNull':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <input
            className="input-fields-number-input"
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
            disabled={isFieldDisabled}
          />
        </>
      )

    case 'integerFromZeroOrAuto(-1)':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <div>
            <input
              className="input-fields-number-input"
              type="number"
              value={
                setting.value === -1
                  ? '' // Shows empty string when isFieldDisabled (Auto is checked)
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
              disabled={setting.value === -1 || isFieldDisabled}
            />
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
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
                disabled={isFieldDisabled}
              />
              Auto
            </label>
          </div>
        </>
      )

    case 'integerFromOneOrAuto(-1)':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>

          <div>
            <input
              className="input-fields-number-input"
              type="number"
              value={
                setting.value === -1 || setting.value === 0
                  ? '' // Shows empty string when isFieldDisabled (Auto is checked)
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
              disabled={
                setting.value === 0 || setting.value === -1 || isFieldDisabled
              }
            />
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
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
                disabled={isFieldDisabled}
              />
              Auto
            </label>
          </div>
        </>
      )

    case 'integerFromOneOrDefaultFromConfig(null)':
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>

          <div>
            <input
              className="input-fields-number-input"
              type="number"
              value={
                setting.value === null
                  ? '' // Shows empty string when isFieldDisabled (Default from Config is checked)
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
              disabled={setting.value === null || isFieldDisabled}
            />
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
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
                disabled={isFieldDisabled}
              />
              Default from Config
            </label>
          </div>
        </>
      )

    case 'time': {
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
          <input
            className="input-fields-text-input"
            type="time"
            step="1"
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
            disabled={isFieldDisabled}
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
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
          </label>
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
