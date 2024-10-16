import { useEffect, useRef, useState } from 'react'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import './DropdownRadio.scss'

interface DropdownRadioProps {
  items: string[]
  title: string
  radioName: string
  badgeContent?: string[]
  onSelect: (selectedItems: string[]) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
}

function DropdownRadio({
  items,
  title,
  radioName,
  badgeContent,
  onSelect,
  isOpen,
  onToggle
}: DropdownRadioProps) {
  const [selectedItem, setSelectedItem] = useState<string | null>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleSelect = (item: string) => {
    if (selectedItem === item) {
      setSelectedItem(null)
      onSelect([])
    } else {
      setSelectedItem(item)
      onSelect([item])
    }
  }

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

  return (
    <div className="radio-dropdown" ref={dropdownRef}>
      <div
        className="radio-dropdown-selected-item"
        onClick={() => onToggle(!isOpen)}
      >
        {title}
        {badgeContent && selectedItem ? (
          <span className="count-badge">
            {badgeContent[items.indexOf(selectedItem)]}
          </span>
        ) : (
          <span className="count-badge-placeholder" />
        )}

        <div className="chevron-container">
          {isOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </div>

      {isOpen && (
        <div className="menu">
          <ul>
            {Array.isArray(items) && items.length ? (
              items.map((item, index) => (
                <li key={index} onClick={() => handleSelect(item)}>
                  <div className="item-content">
                    <input
                      type="checkbox"
                      name={radioName}
                      className="checkbox"
                      checked={selectedItem === item}
                      onChange={() => {}}
                    />
                    <span className="item-text">{item}</span>
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

export default DropdownRadio
