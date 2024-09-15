import React, { useState } from 'react'
import './DropdownSearch.scss'
import FilterFunnel from '../assets/icons/FilterFunnel'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import ChevronRight from '../assets/icons/ChevronRight'

interface DropdownSearchProps {
  items: string[]
  initialTitle: string
  placeholder?: string
  leftwards?: boolean
  onSelect: (item: string) => void
}

const DropdownSearch: React.FC<DropdownSearchProps> = ({
  items,
  initialTitle,
  placeholder = 'Search...',
  leftwards,
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
    <div className="search-dropdown">
      <button onClick={() => setIsOpen(!isOpen)}>
        {selectedItem || initialTitle}
        <div className="chevron-container">
          {isOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>

      {isOpen && (
        <div className={leftwards ? 'menu leftwards' : 'menu'}>
          <div className="search">
            <span className="icon">
              <FilterFunnel />
            </span>
            <input
              type="text"
              value={searchTerm}
              onChange={handleInputChange}
              placeholder={placeholder}
            />
          </div>

          <ul>
            {filteredItems.length ? (
              filteredItems.map((item, index) => (
                <li key={index} onClick={() => handleSelect(item)}>
                  <div className="item-content">
                    <span className="item-text">{item}</span>
                    {!leftwards && (
                      <span className="chevron">
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

export default DropdownSearch
