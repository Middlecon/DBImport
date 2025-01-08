import React, { ReactNode, useState, useEffect, useRef } from 'react'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import './HeaderActions.scss'

interface DropdownActionItem {
  icon: ReactNode
  label: string
  onClick: () => void
}

interface DropdownActionsProps {
  items: DropdownActionItem[]
  isDropdownActionsOpen: boolean
  disabled: boolean
  onToggle: (isDropdownActionsOpen: boolean) => void
}

function DropdownActions({
  items,
  isDropdownActionsOpen,
  disabled,
  onToggle
}: DropdownActionsProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>) => {
    if (!isDropdownActionsOpen) return

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault()
        setActiveIndex((prev) =>
          prev === null || prev === items.length - 1 ? 0 : prev + 1
        )
        break
      case 'ArrowUp':
        event.preventDefault()
        setActiveIndex((prev) =>
          prev === null || prev === 0 ? items.length - 1 : prev - 1
        )
        break

      case 'Enter':
        event.preventDefault()
        if (activeIndex !== null) {
          // Simulate a click on the highlighted item
          items[activeIndex].onClick()
        }
        break

      case 'Escape':
        onToggle(false)
        break
    }
  }

  useEffect(() => {
    if (!isDropdownActionsOpen) {
      setActiveIndex(null)
    }
  }, [isDropdownActionsOpen])

  return (
    <>
      <button
        className="action-dropdown-button"
        onClick={() => onToggle(!isDropdownActionsOpen)}
        onKeyDown={handleKeyDown}
        disabled={disabled}
      >
        Actions
        <div className="action-dropdown-chevron-container">
          {isDropdownActionsOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>

      {isDropdownActionsOpen && (
        <div className="actions-dropdown-wrapper" ref={dropdownRef}>
          {items.map((item, index) => (
            <div
              key={index}
              className={`actions-dropdown-item ${
                activeIndex === index ? 'highlighted' : ''
              } ${index === 0 ? 'add-table' : 'discover'}`}
              onClick={item.onClick}
              tabIndex={0}
            >
              {item.icon} {item.label}
            </div>
          ))}
        </div>
      )}
    </>
  )
}

export default DropdownActions
