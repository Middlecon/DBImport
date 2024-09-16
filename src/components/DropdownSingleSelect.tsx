import React, { useState } from 'react'
import './DropdownSingleSelect.scss'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'

interface DropdownRadioProps {
  items: string[]
  title: string
  radioName: string
  badgeContent?: string[]
  onSelect: (selectedItems: string[]) => void
}

const DropdownRadio: React.FC<DropdownRadioProps> = ({
  items,
  title,
  radioName,
  badgeContent,

  onSelect
}) => {
  const [isOpen, setIsOpen] = useState(false)
  const [selectedItem, setSelectedItem] = useState<string | null>(null)

  const handleSelect = (item: string) => {
    console.log('selectedItem', selectedItem)
    if (selectedItem === item) {
      setSelectedItem(null) // Unselect if the same item is clicked again
      onSelect([]) // Pass an empty array to signify unselection
    } else {
      setSelectedItem(item) // Select the new item
      onSelect([item])
    }
  }

  // const handleSelect = (item: string) => {
  //   setSelectedItem(item)
  //   onSelect([item])
  // }

  return (
    <div className="select-dropdown">
      <button onClick={() => setIsOpen(!isOpen)}>
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
