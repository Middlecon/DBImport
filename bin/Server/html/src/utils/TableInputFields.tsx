import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getAllTimezones } from 'countries-and-timezones'
import Dropdown from '../components/Dropdown'
import { EditSetting } from './interfaces'
import './TableInputFields.scss'
import { useAtom } from 'jotai'
import { airflowTypeAtom } from '../atoms/atoms'
import { validateEmails } from './functions'
import { useLocation } from 'react-router-dom'

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
  isAirflowTasksSensorPokeAndSoftDisabled?: boolean
  isAirflowTasksSensorConnectionDisabled?: boolean
  isAirflowTasksSudoUserDisabled?: boolean
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
  isAirflowTasksSensorPokeAndSoftDisabled = true,
  isAirflowTasksSensorConnectionDisabled = true,
  isAirflowTasksSudoUserDisabled = true,
  disabled = false
}: TableInputFieldsProps) {
  const location = useLocation()
  const pathnames = location.pathname.split('/').filter((x) => x)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const [airflowType] = useAtom(airflowTypeAtom)
  const retriesDefaultValue = useMemo(() => {
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
    setting.label === 'Target Table' ||
    setting.label === 'Target Schema' ||
    setting.label === 'Connection' ||
    setting.label === 'Connection String' ||
    setting.label === 'DAG Name' ||
    setting.label === 'Task Name' ||
    setting.label === 'Retries' ||
    setting.label === 'Filter Connection' ||
    (pathnames[0] === 'import' && setting.label === 'SQL Sessions') ||
    pathnames[1] === 'global'

  const showRequiredIndicator =
    (setting.value === null || setting.value === '') && isRequired

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

  useEffect(() => {
    const inputElement = inputRef.current
    if (inputElement) {
      const handleWheel = (event: WheelEvent) => event.preventDefault()

      inputElement.addEventListener('wheel', handleWheel, { passive: false })

      // Cleanup event listener on component unmount
      return () => {
        inputElement.removeEventListener('wheel', handleWheel)
      }
    }
  }, [])

  switch (setting.type) {
    case 'boolean':
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

      if (setting.label === 'Sensor Soft Fail') {
        return (
          <>
            <label
              className={
                isAirflowTasksSensorPokeAndSoftDisabled
                  ? 'input-fields-label-disabled'
                  : ''
              }
            >
              {setting.label}:
            </label>
            <div className="radio-edit">
              <label
                className={
                  isAirflowTasksSensorPokeAndSoftDisabled
                    ? 'input-fields-label-disabled'
                    : ''
                }
              >
                <input
                  type="radio"
                  name={`booleanNumber-${index}`}
                  value="true"
                  checked={setting.value === 1}
                  onChange={() => handleInputChange(index, 1)}
                  disabled={isAirflowTasksSensorPokeAndSoftDisabled}
                />
                True
              </label>
              <label
                className={
                  isAirflowTasksSensorPokeAndSoftDisabled
                    ? 'input-fields-label-disabled'
                    : ''
                }
              >
                <input
                  type="radio"
                  name={`booleanNumber-${index}`}
                  value="false"
                  checked={setting.value === 0}
                  onChange={() => handleInputChange(index, 0)}
                  disabled={isAirflowTasksSensorPokeAndSoftDisabled}
                />
                False
              </label>
            </div>
          </>
        )
      }
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
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
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
      if (setting.isHidden === true) return null

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
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
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
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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
              onChange={(event) => {
                const value = event.target.value
                handleInputChange(index, value.trim() === '' ? '' : value)
              }}
              onBlur={(event) => {
                const trimmedValue = event.target.value.trim()
                handleInputChange(
                  index,
                  trimmedValue === '' ? prevValue : trimmedValue
                )
              }}
              disabled={isCustomQueryDisabled}
            />
          </>
        )
      }
      if (setting.label === 'Sensor Connection') {
        return (
          <>
            <label
              className={
                isAirflowTasksSensorConnectionDisabled
                  ? 'input-fields-label-disabled'
                  : ''
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
              onChange={(event) => {
                const value = event.target.value
                handleInputChange(index, value.trim() === '' ? '' : value)
              }}
              onBlur={(event) => {
                const trimmedValue = event.target.value.trim()
                handleInputChange(
                  index,
                  trimmedValue === '' ? prevValue : trimmedValue
                )
              }}
              disabled={isAirflowTasksSensorConnectionDisabled}
            />
          </>
        )
      }
      if (pathnames[3] === 'tasks' && setting.label === 'Sudo User') {
        return (
          <>
            <label
              className={
                isAirflowTasksSudoUserDisabled
                  ? 'input-fields-label-disabled'
                  : ''
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
              onChange={(event) => {
                const value = event.target.value
                handleInputChange(index, value.trim() === '' ? '' : value)
              }}
              onBlur={(event) => {
                const trimmedValue = event.target.value.trim()
                handleInputChange(
                  index,
                  trimmedValue === '' ? prevValue : trimmedValue
                )
              }}
              disabled={isAirflowTasksSudoUserDisabled}
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
            onChange={(event) => {
              const value = event.target.value
              handleInputChange(index, value.trim() === '' ? '' : value)
            }}
            onBlur={(event) => {
              const trimmedValue = event.target.value.trim()
              handleInputChange(
                index,
                trimmedValue === '' ? prevValue : trimmedValue
              )
            }}
            required={isRequired}
            disabled={isFieldDisabled}
          />
        </>
      )
    }

    case 'textTripletOctalValue': {
      if (setting.isHidden === true) return null

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
            onBlur={(event) => {
              if (event.target.value === '') {
                handleInputChange(index, prevValue)
              }
            }}
            required={isRequired}
            disabled={isFieldDisabled}
          />
        </>
      )
    }

    case 'textarea': {
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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
      if (setting.isHidden === true) return null

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

    case 'integerOneOrTwo':
      if (setting.isHidden === true) return null

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
                name={`integer-${index}`}
                value="true"
                checked={setting.value === 1}
                onChange={() => handleInputChange(index, 1)}
                disabled={isFieldDisabled}
              />
              1
            </label>
            <label
              className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
            >
              <input
                type="radio"
                name={`integer-${index}`}
                value="false"
                checked={setting.value === 2}
                onChange={() => handleInputChange(index, 2)}
                disabled={isFieldDisabled}
              />
              2
            </label>
          </div>
        </>
      )

    case 'integerFromZero':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <input
            ref={inputRef}
            className="input-fields-number-input"
            type="number"
            value={
              setting.value === ''
                ? ''
                : setting.value !== null && setting.value !== undefined
                ? Number(setting.value)
                : ''
              // setting.value !== null && setting.value !== undefined
              //   ? Number(setting.value)
              //   : retriesDefaultValue
            }
            onChange={(event) => {
              const value =
                event.target.value === '' ? '' : Number(event.target.value)

              // Temporarily allows empty input, otherwise enforce default value
              handleInputChange(
                index,
                value === '' || Number(value) >= 0 ? value : retriesDefaultValue
              )
            }}
            onBlur={(event) => {
              const value = Number(event.target.value)

              // Reverts to prevValue if the input is empty or less than 1
              if (event.target.value === '' || isNaN(value) || value < 1) {
                handleInputChange(index, prevValue)
              }
            }}
            // onChange={(event) => {

            //   if (
            //     setting.label === 'Atlas Discovery Interval' &&
            //     Number(value) > 24
            //   )
            //     value = prevValue as number

            //   // Temporarily allows empty input, otherwise enforce minimum of 1
            //   handleInputChange(
            //     index,
            //     value === '' || Number(value) >= 1 ? value : 1
            //   )
            // }}
            onKeyDown={(event) => {
              // Prevent invalid characters from being typed
              if (['e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key)) {
                event.preventDefault()
              }
            }}
            step="1"
            disabled={isFieldDisabled}
            required={isRequired}
          />
        </>
      )

    // case 'integerFromZero':
    //   if (setting.isHidden === true) return null

    //   return (
    //     <>
    //       <label
    //         className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
    //       >
    //         {setting.label}:
    //         {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
    //       </label>
    //       <input
    //         className="input-fields-number-input"
    //         type="number"
    //         value={
    //           setting.value !== null && setting.value !== undefined
    //             ? Number(setting.value)
    //             : retriesDefaultValue
    //         }
    //         onChange={(event) => {
    //           let value: string | number | null =
    //             event.target.value === '' ? '' : Number(event.target.value)
    //           if (typeof value === 'number' && value < 0) {
    //             value = retriesDefaultValue
    //           }

    //           handleInputChange(index, value === '' ? null : value)
    //         }}
    //         onBlur={(event) => {
    //           const value = event.target.value
    //           // If input is empty or not a valid number greater than 1, set to default 5
    //           if (value === '' || isNaN(Number(value)) || Number(value) < 0) {
    //             handleInputChange(index, retriesDefaultValue)
    //             event.target.value = ''
    //           }
    //         }}
    //         onKeyDown={(event) => {
    //           // Prevent invalid characters from being typed
    //           if (
    //             ['e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key) // Dead is still working, fix so it is not
    //           ) {
    //             event.preventDefault()
    //           }
    //         }}
    //         step="1"
    //         disabled={isFieldDisabled}
    //         required={isRequired}
    //       />
    //     </>
    //   )

    case 'integerFromZeroOrNull':
      if (setting.isHidden === true) return null

      if (setting.label === 'Sensor Poke Interval') {
        return (
          <>
            <label
              className={
                isAirflowTasksSensorPokeAndSoftDisabled
                  ? 'input-fields-label-disabled'
                  : ''
              }
            >
              {setting.label}:
            </label>
            <input
              ref={inputRef}
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
              disabled={isAirflowTasksSensorPokeAndSoftDisabled}
            />
          </>
        )
      }
      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <input
            ref={inputRef}
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
            required={isRequired}
          />
        </>
      )

    case 'integerFromZeroOrAuto(-1)':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <div>
            <input
              ref={inputRef}
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

    case 'integerFromOne':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <div>
            <input
              ref={inputRef}
              className="input-fields-number-input"
              type="number"
              value={
                setting.value === ''
                  ? ''
                  : setting.value !== null && setting.value !== undefined
                  ? Number(setting.value)
                  : ''
              }
              onChange={(event) => {
                let value =
                  event.target.value === '' ? '' : Number(event.target.value)
                if (
                  setting.label === 'Atlas Discovery Interval' &&
                  Number(value) > 24
                )
                  value = prevValue as number

                // Temporarily allows empty input, otherwise enforce minimum of 1
                handleInputChange(
                  index,
                  value === '' || Number(value) >= 1 ? value : 1
                )
              }}
              onBlur={(event) => {
                const value = Number(event.target.value)

                // Reverts to prevValue if the input is empty or less than 1
                if (event.target.value === '' || isNaN(value) || value < 1) {
                  handleInputChange(index, prevValue)
                }
              }}
              step="1"
              required={isRequired}
            />
          </div>
        </>
      )

    case 'integerFromOneOrNull':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <input
            ref={inputRef}
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
            required={isRequired}
          />
        </>
      )

    case 'integerFromOneOrAuto(-1)':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <div>
            <input
              ref={inputRef}
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
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>

          <div>
            <input
              ref={inputRef}
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

    case 'time':
      if (setting.isHidden === true) return null

      return (
        <>
          <label
            className={isFieldDisabled ? 'input-fields-label-disabled' : ''}
          >
            {setting.label}:
            {showRequiredIndicator && <span style={{ color: 'red' }}>*</span>}
          </label>
          <input
            className="input-fields-text-input"
            type="time"
            step="1"
            value={setting.value ? String(setting.value) : ''}
            onChange={(event) => handleInputChange(index, event.target.value)}
            disabled={isFieldDisabled}
            required={isRequired}
          />
        </>
      )

    case 'timezone': {
      if (setting.isHidden === true) return null

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
