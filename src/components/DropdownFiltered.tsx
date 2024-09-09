import React, { useState } from 'react'
import './DropdownFiltered.scss'
import FilterFunnel from '../assets/icons/FilterFunnel'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import ChevronRight from '../assets/icons/ChevronRight'

interface DropdownFilteredProps {
  items: string[]
  initialTitle: string
  placeholder?: string
  onSelect: (item: string) => void
}

const DropdownFiltered: React.FC<DropdownFilteredProps> = ({
  items,
  initialTitle,
  placeholder = 'Search...',
  onSelect
}) => {
  const [searchTerm, setSearchTerm] = useState('')
  const [isOpen, setIsOpen] = useState(false)
  const [selectedItem, setSelectedItem] = useState<string | null>(null)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(e.target.value)
  }

  const handleSelect = (item: string) => {
    setSelectedItem(item)
    onSelect(item)
    setSearchTerm('')
    setIsOpen(false)
  }

  const filteredItems = items.filter((item) =>
    item.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="filter-dropdown">
      <button
        className="filter-dropdown__button"
        onClick={() => setIsOpen(!isOpen)}
      >
        {selectedItem || initialTitle}
        <div className="filter-dropdown__chevron">
          {isOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>

      {isOpen && (
        <div className="filter-dropdown__menu">
          <div className="filter-dropdown__search">
            <span className="filter-dropdown__icon">
              <FilterFunnel />
            </span>
            <input
              type="text"
              className="filter-dropdown__input"
              value={searchTerm}
              onChange={handleInputChange}
              placeholder={placeholder}
            />
          </div>
          <ul className="filter-dropdown__list">
            {filteredItems.length ? (
              filteredItems.map((item) => (
                <li
                  key={item}
                  className="filter-dropdown__item"
                  onClick={() => handleSelect(item)}
                >
                  {item}
                  <span className="filter-dropdown__chevron">
                    <ChevronRight />
                  </span>
                </li>
              ))
            ) : (
              <li className="filter-dropdown__no-results" no-hover>
                No results found
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  )
}

export default DropdownFiltered
