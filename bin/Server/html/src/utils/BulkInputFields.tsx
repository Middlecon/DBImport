import React, { useMemo, useCallback, useState } from 'react'
import Dropdown from '../components/Dropdown'
import InfoText from '../components/InfoText'
import { BulkField } from './interfaces'
import { SettingType } from './enums'
import './TableInputFields.scss'
import { nameDisplayMappings } from './nameMappings'

export interface BulkInputFieldsProps<T> {
  fields: BulkField<T>[]
  selectedRows: T[]
  bulkChanges: Partial<Record<keyof T, string | number | boolean | null>>
  onBulkChange: (
    fieldKey: keyof T,
    newValue: string | number | boolean | null
  ) => void
  disabled?: boolean
}

function BulkInputFields<T>({
  fields,
  selectedRows,
  bulkChanges,
  onBulkChange,
  disabled = false
}: BulkInputFieldsProps<T>) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const handleSelect = useCallback(
    (fieldKey: keyof T, newValue: string | number | boolean | null) => {
      console.log('fieldKey', fieldKey)
      console.log('newValue', newValue)
      onBulkChange(fieldKey, newValue)
    },
    [onBulkChange]
  )

  const fieldValues = useMemo(() => {
    return fields.map((field) => {
      const values = selectedRows.map((row) => {
        if (field.key in bulkChanges) {
          return bulkChanges[field.key] as T[keyof T]
        }
        return row[field.key]
      })

      const firstValue = values[0]
      const allSame = values.every((val) => val === firstValue)
      return {
        ...field,
        allSame,
        commonValue: allSame ? firstValue : null
      }
    })
  }, [fields, selectedRows, bulkChanges])

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  return (
    <div className="bulk-input-fields-container">
      {fieldValues.map((field) => {
        const {
          key,
          label,
          type,
          enumOptions,
          infoText,
          allSame,
          commonValue,
          isRequired
        } = field

        switch (type) {
          case SettingType.Enum: {
            const dropdownOptions = enumOptions
              ? Object.values(enumOptions)
              : []

            const dropdownId = `dropdown-${String(field.key)}`

            const currentValue =
              key in bulkChanges
                ? bulkChanges[key]
                : allSame
                ? commonValue
                : null

            const currentTitle =
              currentValue != null
                ? nameDisplayMappings[String(key)]?.[String(currentValue)] ??
                  String(currentValue)
                : 'Select...'

            return (
              <div className="bulk-input-row" key={String(key)}>
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <Dropdown
                  keyLabel={label}
                  items={dropdownOptions}
                  onSelect={(item) => handleSelect(key, item)}
                  isOpen={openDropdown === dropdownId}
                  onToggle={(isOpen: boolean) =>
                    handleDropdownToggle(dropdownId, isOpen)
                  }
                  searchFilter={false}
                  initialTitle={currentTitle}
                  backgroundColor="inherit"
                  textColor="black"
                  fontSize="14px"
                  border="0.5px solid rgb(42,42,42)"
                  borderRadius="3px"
                  height="21.5px"
                  chevronWidth="11"
                  chevronHeight="7"
                  lightStyle={true}
                  disabled={disabled}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
              </div>
            )
          }

          case SettingType.Text: {
            return (
              <div className="bulk-input-row" key={String(key)}>
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <input
                  type="text"
                  placeholder={!allSame ? '(multiple values)' : ''}
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  onChange={(e) => handleSelect(key, e.target.value)}
                  disabled={disabled}
                  required={isRequired}
                  className="input-fields-text-input"
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={430}
                    isInfoTextPositionUp={false}
                  />
                )}
              </div>
            )
          }

          case SettingType.Boolean: {
            const boolValue = allSame ? Boolean(commonValue) : null
            console.log('boolValue', boolValue)
            return (
              <div className="bulk-input-row" key={String(key)}>
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <div className="radio-edit">
                  <label>
                    <input
                      type="radio"
                      name={`boolean-${String(key)}`}
                      value="true"
                      checked={boolValue === true}
                      onChange={() => handleSelect(key, true)}
                      disabled={disabled}
                    />
                    True
                  </label>
                  <label>
                    <input
                      type="radio"
                      name={`boolean-${String(key)}`}
                      value="false"
                      checked={boolValue === false && boolValue !== null}
                      onChange={() => handleSelect(key, false)}
                      disabled={disabled}
                    />
                    False
                  </label>
                </div>
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
              </div>
            )
          }

          case SettingType.Readonly: {
            return (
              <div className="bulk-input-row" key={String(key)}>
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <span>
                  {allSame && commonValue !== null
                    ? String(commonValue)
                    : '(multiple values)'}
                </span>
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
              </div>
            )
          }

          case SettingType.GroupingSpace: {
            return <div key={String(key)} className="setting-grouping-space" />
          }

          default:
            return null
        }
      })}
    </div>
  )
}

export default React.memo(BulkInputFields)
