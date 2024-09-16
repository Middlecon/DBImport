import React, { useState } from 'react'
import './DropdownCheckbox.scss'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'

interface DropdownCheckboxProps {
  items: string[]
  title: string
  onSelect: (selectedItems: string[]) => void
}

const DropdownCheckbox: React.FC<DropdownCheckboxProps> = ({
  items,
  title,

  onSelect
}) => {
  const [isOpen, setIsOpen] = useState(false)
  const [selectedItems, setSelectedItems] = useState<string[]>([])

  const handleSelect = (item: string) => {
    const newSelectedItems = selectedItems.includes(item)
      ? selectedItems.filter((i) => i !== item)
      : [...selectedItems, item]

    setSelectedItems(newSelectedItems)
    onSelect(newSelectedItems)
  }

  return (
    <div className="checkbox-dropdown">
      <button onClick={() => setIsOpen(!isOpen)}>
        {title}
        {selectedItems.length > 0 ? (
          <span className="count-badge">{selectedItems.length}</span>
        ) : (
          <span className="count-badge-placeholder" />
        )}

        <div className="chevron-container">
          {isOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>

      {isOpen && (
        <div className="menu">
          <ul>
            {items.length ? (
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
