import { CSSProperties, useEffect, useRef, useState } from 'react'
import InfoIcon from '../assets/icons/InfoIcon'
import './InfoText.scss'

interface InfoTextProps {
  label: string
  infoText: string
  infoTextMaxWidth?: number
  isInfoTextPositionUp?: boolean
  iconPosition?: CSSProperties
}

function InfoText({
  label,
  infoText,
  infoTextMaxWidth,
  isInfoTextPositionUp,
  iconPosition = { paddingTop: 2 }
}: InfoTextProps) {
  const [openInfoDropdown, setOpenInfoDropdown] = useState<string | null>(null)
  const [isHovered, setIsHovered] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const infoTextRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setOpenInfoDropdown(null)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [])

  useEffect(() => {
    const handleSelectAll = (event: KeyboardEvent) => {
      // Checks if Cmd+A or Ctrl+A is pressed and if infoText is open or hovered
      if (
        (openInfoDropdown === label || isHovered) &&
        (event.metaKey || event.ctrlKey) &&
        event.key === 'a'
      ) {
        event.preventDefault()
        if (infoTextRef.current) {
          const range = document.createRange()
          range.selectNodeContents(infoTextRef.current)
          const selection = window.getSelection()
          selection?.removeAllRanges()
          selection?.addRange(range)
        }
      }
    }

    document.addEventListener('keydown', handleSelectAll)
    return () => {
      document.removeEventListener('keydown', handleSelectAll)
    }
  }, [openInfoDropdown, isHovered, label])

  const handleInfoDropdownToggle = (dropdownId: string) => {
    setOpenInfoDropdown((prev) => (prev === dropdownId ? null : dropdownId))
  }

  const handleMouseEnter = () => setIsHovered(true)
  const handleMouseLeave = () => setIsHovered(false)

  const shouldShowInfo = openInfoDropdown === label || isHovered
  console.log('isInfoTextPositionUp', isInfoTextPositionUp)
  return (
    <div className="info-text-container">
      <div
        className="info-icon"
        onClick={() => handleInfoDropdownToggle(label)}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        ref={containerRef}
        style={iconPosition}
      >
        <InfoIcon />
        {shouldShowInfo && (
          <div
            className="info-expanded-content"
            style={
              isInfoTextPositionUp
                ? { top: 'auto', bottom: '100%', maxWidth: infoTextMaxWidth }
                : { maxWidth: infoTextMaxWidth }
            }
          >
            <div className="info-text" ref={infoTextRef}>
              {infoText}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default InfoText
