import { useEffect, useRef, useState } from 'react'
import { SettingType } from '../../../../utils/enums'
import './Setting.scss'
import ChevronDown from '../../../../assets/icons/ChevronDown'
import ChevronUp from '../../../../assets/icons/ChevronUp'

interface SettingProps {
  label: string
  value: string | number | boolean | null
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

      setHasOverflow(isOverflowing)
    }
  }, [value])

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setOpenDropdown(null)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [])

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }
  const renderSetting = () => {
    switch (type) {
      case 'boolean':
        return <span>{value ? 'True' : 'False'}</span>

      case 'booleanNumber':
        if (value === 1) {
          return <span>True</span>
        } else {
          return <span>False</span>
        }
      case 'booleanOrDefaultFromConfig(-1)':
        if (value === 1) {
          return <span>True</span>
        } else if (value === 0) {
          return <span>False</span>
        } else {
          return <span>Default from config</span>
        }

      case 'booleanOrDefaultFromConnection(-1)':
        if (value === 1) {
          return <span>True</span>
        } else if (value === 0) {
          return <span>False</span>
        } else {
          return <span>Default from connnection</span>
        }

      case 'readonly':
        return <span>{value}</span>

      case 'text':
        return <span>{value}</span>

      case 'enum':
        return <span>{value}</span>

      case 'reference':
        return <span>{value}</span>
      case 'hidden':
        return null

      case 'integerFromZeroOrNull':
        return <span>{value}</span>

      case 'integerFromOneOrNull':
        return <span>{value}</span>

      case 'integerFromZeroOrAuto(-1)':
        if (typeof value === 'number' && value > -1) {
          return <span>{value}</span>
        } else {
          return <span>Auto</span>
        }

      case 'integerFromOneOrAuto(-1)':
        if (typeof value === 'number' && value > -1) {
          return <span>{value}</span>
        } else {
          return <span>Auto</span>
        }

      case 'integerFromOneOrDefaultFromConfig(null)':
        if (typeof value === 'number' && value !== null) {
          return <span>{value}</span>
        } else {
          return <span>Default from config</span>
        }
      case 'groupingSpace':
        return <div className="setting-grouping-space"> </div>

      default:
        return null
    }
  }

  return (
    <div className={isConditionsMet === false ? 'setting-disabled' : 'setting'}>
      <dt className="setting-label">
        {label}
        {type !== 'groupingSpace' && ':'}
      </dt>
      <dd
        className={type !== 'groupingSpace' ? 'setting-container' : ''}
        ref={containerRef}
      >
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
