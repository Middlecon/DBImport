import React from 'react'
import { FieldType } from '../../../../utils/enums'

interface FieldProps {
  label: string
  value: string | number | boolean
  type: FieldType
  enumOptions?: { [key: string]: string } // Maybe not needed here
  hidden?: boolean
}

const Field: React.FC<FieldProps> = ({ label, value, type }) => {
  // if (type === 'enum') {
  //   console.log('value', value)
  //   console.log('typeof value', typeof value)
  //   console.log('enumOptions', enumOptions)
  // }
  const renderField = () => {
    switch (type) {
      case 'boolean':
        return <span>{value ? 'True' : 'False'}</span>
      case 'readonly':
        return <span>{value}</span>

      case 'text':
        return <span>{value}</span>

      case 'enum':
        return <span>{value}</span>
      case 'integer':
        return <span>{value}</span>
      case 'hidden':
        return null
      case 'booleanOrAuto(-1)':
        if (typeof value === 'boolean') {
          return <span>{value ? 'True' : 'False'}</span>
        } else {
          return <span>Auto</span>
        }
      case 'integerOrAuto(-1)':
        if (typeof value === 'number' && value > -1) {
          return <span>{value}</span>
        } else {
          return <span>Auto</span>
        }

      case 'booleanOrDefaultFromConfig(-1)':
        if (typeof value === 'boolean') {
          return <span>{value ? 'True' : 'False'}</span>
        } else {
          return <span>Default from config</span>
        }

      default:
        return null
    }
  }

  return (
    <div className="field">
      <label>{label}:</label>
      {renderField()}
    </div>
  )
}

export default Field
