import { useEffect, useRef, useState } from 'react'
import './Dropdown.scss'
import FilterFunnel from '../assets/icons/FilterFunnel'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import ChevronRight from '../assets/icons/ChevronRight'
import { TableSetting } from '../utils/interfaces'

interface DropdownProps<T> {
  items: T[]
  onSelect: (item: T, keyLabel?: string) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
  searchFilter: boolean
  keyLabel?: string
  chevron?: boolean
  initialTitle?: string
  placeholder?: string
  leftwards?: boolean
  backgroundColor?: string
  textColor?: string
  fontSize?: string
  border?: string
  borderRadius?: string
  height?: string
  padding?: string
  chevronWidth?: string
  chevronHeight?: string
  lightStyle?: boolean
}

function Dropdown<T>({
  items,
  onSelect,
  isOpen,
  onToggle,
  searchFilter = true,
  keyLabel,
  chevron = false,
  initialTitle,
  placeholder = 'Search...',
  leftwards,
  backgroundColor,
  textColor,
  fontSize,
  border,
  borderRadius,
  height,
  padding,
  chevronWidth,
  chevronHeight,
  lightStyle
}: DropdownProps<T>): JSX.Element {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedItem, setSelectedItem] = useState<T | null>(null)
  const [savedScrollPosition, setSavedScrollPosition] = useState<number>(0)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        onToggle(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [onToggle])

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value

    if (menuRef.current) {
      if (value) {
        setSavedScrollPosition(menuRef.current.scrollTop)
        menuRef.current.scrollTop = 0
      } else {
        menuRef.current.scrollTop = savedScrollPosition
      }
    }

    setSearchTerm(value)
  }

  const handleSelect = (item: T) => {
    setSelectedItem(item)
    if (keyLabel) {
      onSelect(item, keyLabel)
    } else {
      onSelect(item)
    }
    onToggle(false)
    setSearchTerm('')
  }

  const getItemLabel = (item: T): string => {
    if (typeof item === 'string') {
      return item
    }
    return (item as TableSetting).label
  }

  const filteredItems = items.filter((item) =>
    getItemLabel(item).toLowerCase().includes(searchTerm.toLowerCase())
  )

  const dropdownStyle: React.CSSProperties = {
    backgroundColor: backgroundColor,
    color: textColor,
    fontSize: fontSize,
    border: border,
    borderRadius: borderRadius,
    height: height,
    padding: padding
  }

  return (
    <div className="dropdown" ref={dropdownRef}>
      <div
        className="dropdown-selected-item"
        style={dropdownStyle}
        onClick={() => onToggle(!isOpen)}
      >
        {selectedItem ? getItemLabel(selectedItem) : initialTitle}
        <div className="chevron-container">
          {isOpen ? (
            <ChevronUp
              width={chevronWidth ? chevronWidth : ''}
              height={chevronHeight ? chevronHeight : ''}
            />
          ) : (
            <ChevronDown
              width={chevronWidth ? chevronWidth : ''}
              height={chevronHeight ? chevronHeight : ''}
            />
          )}
        </div>
      </div>

      {isOpen && (
        <div
          ref={menuRef}
          className={
            leftwards
              ? lightStyle
                ? 'light-menu leftwards'
                : 'menu leftwards'
              : lightStyle
              ? 'light-menu'
              : 'menu'
          }
        >
          {searchFilter && (
            <div className="search">
              <span className="icon">
                <FilterFunnel />
              </span>
              <input
                type="text"
                value={searchTerm}
                placeholder={placeholder}
                onChange={handleInputChange}
              />
            </div>
          )}

          <ul>
            {filteredItems.length ? (
              filteredItems.map((item, index) => (
                <li key={index} onClick={() => handleSelect(item)}>
                  <div className="item-content">
                    <span className="item-text">{getItemLabel(item)}</span>
                    {chevron && !leftwards && (
                      <span>
                        <ChevronRight />
                      </span>
                    )}
                  </div>
                </li>
              ))
            ) : (
              <li className="no-results" no-hover>
                No results found
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  )
}

export default Dropdown
