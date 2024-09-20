import { SettingType } from '../../../../utils/enums'
import './Setting.scss'

interface SettingProps {
  label: string
  value: string | number | boolean
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string } // Maybe not needed here
  isHidden?: boolean
}

const Setting: React.FC<SettingProps> = ({
  label,
  value,
  type,
  isConditionsMet
}) => {
  // if (type === 'enum') {
  //   console.log('value', value)
  //   console.log('typeof value', typeof value)
  //   console.log('enumOptions', enumOptions)
  // }
  console.log('isConditionsMet', isConditionsMet)
  const renderSetting = () => {
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
    <div className={isConditionsMet === false ? 'disabled-setting' : 'setting'}>
      <dt className="setting-label">{label}:</dt>
      <dd className="setting-container">{renderSetting()}</dd>
    </div>
  )
}

export default Setting
