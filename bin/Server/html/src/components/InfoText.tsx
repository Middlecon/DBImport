import { useEffect, useRef, useState } from 'react'
import InfoIcon from '../assets/icons/InfoIcon'
import './InfoText.scss'

interface InfoTextProps {
  label: string
  infoText: string
}

function InfoText({ label, infoText }: InfoTextProps) {
  const [openInfoDropdown, setOpenInfoDropdown] = useState<string | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)

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

  const handleInfoDropdownToggle = (dropdownId: string) => {
    setOpenInfoDropdown((prev) => (prev === dropdownId ? null : dropdownId))
  }

  return (
    <div className="info-text-container">
      <div
        className="info-icon"
        onClick={() => handleInfoDropdownToggle(label)}
        ref={containerRef}
      >
        <InfoIcon />
        {openInfoDropdown && (
          <div className="expanded-content">
            <div className="info-text">{infoText}</div>
          </div>
        )}
      </div>
    </div>
  )
}

export default InfoText
