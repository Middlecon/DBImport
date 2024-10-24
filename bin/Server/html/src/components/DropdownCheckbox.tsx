import { useEffect, useRef } from 'react'
import './DropdownCheckbox.scss'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'

interface DropdownCheckboxProps {
  items: string[]
  title: string
  selectedItems: string[]
  onSelect: (selectedItems: string[]) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
}

function DropdownCheckbox({
  items,
  title,
  selectedItems,
  onSelect,
  isOpen,
  onToggle
}: DropdownCheckboxProps) {
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleSelect = (item: string) => {
    const newSelectedItems = selectedItems.includes(item)
      ? selectedItems?.filter((i) => i !== item) ?? []
      : [...selectedItems, item]

    onSelect(newSelectedItems)
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
    <div className="checkbox-dropdown" ref={dropdownRef}>
      <div
        className="checkbox-dropdown-selected-item"
        onClick={() => onToggle(!isOpen)}
      >
        {title}
        {Array.isArray(selectedItems) && selectedItems.length > 0 ? (
          <span className="count-badge">{selectedItems.length}</span>
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
                      className="checkbox"
                      checked={selectedItems.includes(item)}
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

export default DropdownCheckbox
