import { useCallback, useEffect, useRef, useState } from 'react'
import debounce from 'lodash/debounce'
import { isString } from 'lodash'
import './Setting.scss'
import ChevronDown from '../../../../assets/icons/ChevronDown'
import ChevronUp from '../../../../assets/icons/ChevronUp'
import { SettingType } from '../../../../utils/enums'
import { useCustomSelection } from '../../../../utils/hooks'

interface SettingProps {
  label: string
  value: string | number | boolean | null
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string } // Maybe not needed here
  valueFieldWidth?: string
  isHidden?: boolean
}

function Setting({
  label,
  value,
  type,
  isConditionsMet,
  valueFieldWidth
}: SettingProps) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [hasOverflow, setHasOverflow] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const maxCharacters = 260

  useCustomSelection(dropdownRef, openDropdown === label)

  const checkOverflow = useCallback(() => {
    if (containerRef.current) {
      const child = containerRef.current.firstElementChild
      const chevronAdjustment = 20

      if (
        typeof value === 'string' &&
        value.length >= maxCharacters &&
        type === 'textarea'
      ) {
        setHasOverflow(true)
      } else {
        const isOverflowing =
          (child?.scrollWidth ?? 0) >
            containerRef.current.clientWidth - chevronAdjustment ||
          (child?.scrollHeight ?? 0) > containerRef.current.clientHeight

        setHasOverflow(isOverflowing)
      }
    }
  }, [value, type])

  useEffect(() => {
    checkOverflow()
    const handleResize = debounce(checkOverflow, 150)
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
    }
  }, [checkOverflow])

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        dropdownRef.current &&
        !containerRef.current.contains(event.target as Node) &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        setOpenDropdown(null)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [])

  const getTruncatedText = (
    value: string | number | boolean | null,
    isDropdownOpen: boolean
  ) => {
    if (!isString(value)) return ''

    return isDropdownOpen || value.length <= maxCharacters
      ? value
      : `${value.slice(0, maxCharacters)}...`
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    setOpenDropdown(isOpen ? dropdownId : null)
  }

  const renderSetting = () => {
    switch (type) {
      case 'boolean':
        return <span>{value ? 'True' : 'False'}</span>
      case 'booleanNumber':
        return <span>{value === 1 ? 'True' : 'False'}</span>
      case 'booleanOrDefaultFromConfig(-1)':
        return (
          <span>
            {value === 1
              ? 'True'
              : value === 0
              ? 'False'
              : 'Default from Config'}
          </span>
        )
      case 'booleanOrDefaultFromConnection(-1)':
        return (
          <span>
            {value === 1
              ? 'True'
              : value === 0
              ? 'False'
              : 'Default from connection'}
          </span>
        )
      case 'readonly':
        return <span>{value}</span>
      case 'text':
        return <span>{getTruncatedText(value, openDropdown === label)}</span>

      case 'textarea':
        if (label === 'Connection String') {
          return <div>{value}</div>
        } else
          return <div>{getTruncatedText(value, openDropdown === label)}</div>

      default:
        return <span>{value}</span>
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
        style={{ width: valueFieldWidth || '195px' }}
        ref={containerRef}
      >
        <div
          className={
            type === 'textarea'
              ? 'collapsed-content-textarea'
              : 'collapsed-content'
          }
          style={{
            visibility: openDropdown === label ? 'hidden' : 'visible'
          }}
        >
          {renderSetting()}
        </div>

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
        <div ref={dropdownRef} className="expanded-content">
          {renderSetting()}
        </div>
      )}
    </div>
  )
}

export default Setting
