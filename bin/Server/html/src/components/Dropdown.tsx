import { useEffect, useRef, useState } from 'react'
import './Dropdown.scss'
import FilterFunnel from '../assets/icons/FilterFunnel'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import ChevronRight from '../assets/icons/ChevronRight'
import { EditSetting } from '../utils/interfaces'
import CloseIcon from '../assets/icons/CloseIcon'

interface DropdownProps<T> {
  items: T[]
  onSelect: (item: T | null, keyLabel?: string) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
  searchFilter: boolean
  keyLabel?: string
  chevron?: boolean
  cross?: boolean
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
  disabled?: boolean
}

function Dropdown<T>({
  items,
  onSelect,
  isOpen,
  onToggle,
  searchFilter = true,
  keyLabel,
  chevron = false,
  cross = false,
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
  lightStyle,
  disabled = false
}: DropdownProps<T>): JSX.Element {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedItem, setSelectedItem] = useState<T | null>(
    (initialTitle as T) || null
  )
  const [savedScrollPosition, setSavedScrollPosition] = useState<number>(0)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const searchInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (disabled || !isOpen) return

    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        onToggle(false)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onToggle(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [disabled, isOpen, onToggle])

  useEffect(() => {
    if (isOpen && searchFilter && searchInputRef.current) {
      searchInputRef.current.focus()
    }
  }, [isOpen, searchFilter])

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (disabled) return

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

  const handleSelect = (item: T | null) => {
    if (disabled) return

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
    return (item as EditSetting).label
  }

  const filteredItems = (
    items?.filter((item) =>
      getItemLabel(item).toLowerCase().includes(searchTerm.toLowerCase())
    ) ?? []
  ).filter((item) => item !== selectedItem)

  const dropdownStyle: React.CSSProperties = {
    backgroundColor,
    color: textColor,
    fontSize,
    border,
    borderRadius,
    height,
    padding,
    cursor: disabled ? 'not-allowed' : 'pointer',
    opacity: disabled ? 0.5 : 1
  }

  const itemContentStyle: React.CSSProperties = {
    padding: dropdownStyle.padding,
    height: dropdownStyle.height
      ? `calc(${dropdownStyle.height} + 5px)`
      : undefined
  }

  const heightStyle: React.CSSProperties = {
    height: dropdownStyle.height
      ? `calc(${dropdownStyle.height} + 7px)`
      : undefined
  }

  return (
    <div className="dropdown" ref={dropdownRef}>
      <div
        className="dropdown-selected-item"
        style={dropdownStyle}
        onClick={() => !disabled && onToggle(!isOpen)}
      >
        {selectedItem ? getItemLabel(selectedItem) : initialTitle}

        <div className="chevron-container">
          {cross && selectedItem && selectedItem !== 'Select...' && !isOpen ? (
            <CloseIcon
              onClick={() => {
                handleSelect(null)
              }}
            />
          ) : isOpen ? (
            <ChevronUp
              width={chevronWidth || ''}
              height={chevronHeight || ''}
            />
          ) : (
            <ChevronDown
              width={chevronWidth || ''}
              height={chevronHeight || ''}
            />
          )}
        </div>
      </div>

      {!disabled && isOpen && (
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
            <div className="search" style={heightStyle}>
              <span className="icon">
                <FilterFunnel />
              </span>
              <input
                type="text"
                value={searchTerm}
                placeholder={placeholder}
                onChange={handleInputChange}
                ref={searchInputRef}
              />
            </div>
          )}

          <ul>
            {Array.isArray(filteredItems) && filteredItems.length ? (
              filteredItems.map((item, index) => (
                <li key={index} onClick={() => handleSelect(item)}>
                  <div className="item-content" style={itemContentStyle}>
                    <span className="item-text">{getItemLabel(item)}</span>
                    {chevron && !leftwards && <ChevronRight />}
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
