import { useEffect, useRef, useState } from 'react'
import { SettingType } from '../../../../utils/enums'
import './Setting.scss'
import ChevronDown from '../../../../assets/icons/ChevronDown'
import ChevronUp from '../../../../assets/icons/ChevronUp'

interface SettingProps {
  label: string
  value: string | number | boolean
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string } // Maybe not needed here
  isHidden?: boolean
}

function Setting({ label, value, type, isConditionsMet }: SettingProps) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [hasOverflow, setHasOverflow] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (containerRef.current) {
      const childWidth =
        containerRef.current.firstElementChild?.scrollWidth || 0
      const isOverflowing = childWidth > containerRef.current.clientWidth

      // console.log('Child content width:', childWidth)
      // console.log('Container width:', containerRef.current.clientWidth)
      // console.log('Is overflowing:', isOverflowing)

      setHasOverflow(isOverflowing)
    }
  }, [value])
  // console.log('hasOverflow', hasOverflow)

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    console.log('dropdownId', dropdownId)
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }
  // console.log('isConditionsMet', isConditionsMet)
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
      case 'reference':
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
      <dd className="setting-container" ref={containerRef}>
        <div className="collapsed-content">{renderSetting()}</div>
        {hasOverflow && !openDropdown && (
          <div
            className="chevron"
            onClick={() => handleDropdownToggle(label, true)}
          >
            <ChevronDown />
          </div>
        )}
        {hasOverflow && openDropdown && (
          <div
            className="chevron"
            onClick={() => handleDropdownToggle(label, false)}
          >
            <ChevronUp />
          </div>
        )}
      </dd>

      {hasOverflow && openDropdown && (
        <>
          <div className="expanded-content">{renderSetting()}</div>
        </>
      )}
    </div>
  )
}

export default Setting
