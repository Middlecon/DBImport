import React, { useEffect, useRef, useState } from 'react'
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
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
}

const DropdownSearch: React.FC<DropdownSearchProps> = ({
  items,
  initialTitle,
  placeholder = 'Search...',
  leftwards,
  onSelect,
  isOpen,
  onToggle
}) => {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedItem, setSelectedItem] = useState<string | null>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

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

  const handleSelect = (item: string) => {
    setSelectedItem(item)
    onSelect(item)
    onToggle(false)
    setSearchTerm('')
  }

  const filteredItems = items.filter((item) =>
    item.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="search-dropdown" ref={dropdownRef}>
      <button onClick={() => onToggle(!isOpen)}>
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
              placeholder={placeholder}
              onChange={(e) => setSearchTerm(e.target.value)}
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
