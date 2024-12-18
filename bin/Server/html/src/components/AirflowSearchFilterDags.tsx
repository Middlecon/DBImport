import { useEffect, useRef, useState } from 'react'
import Dropdown from './Dropdown'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import FilterFunnel from '../assets/icons/FilterFunnel'
import './SearchFilterTables.scss'
import Button from './Button'
import {
  EditSettingValueTypes,
  UiAirflowsSearchFilter
} from '../utils/interfaces'
import FavoriteFilterSearch from './FavoriteFilterSearch'
import { useFocusTrap } from '../utils/hooks'

interface AirflowSearchFilterDagsProps {
  isSearchFilterOpen: boolean
  type: string
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (filters: UiAirflowsSearchFilter) => void
  disabled?: boolean
}

function AirflowSearchFilterDags({
  isSearchFilterOpen,
  type,
  onToggle,
  onShow,
  disabled
}: AirflowSearchFilterDagsProps) {
  const query = new URLSearchParams(location.search)

  const autoRegenerateDag = query.get('autoRegenerateDag') || null

  const [autoRegenerateDagFilter, setAutoRegenerateDagFilter] = useState({
    label: 'Auto Regenerate DAG',
    keyLabel: 'autoRegenerateDag',
    value: autoRegenerateDag,
    items: ['True', 'False']
  })

  const containerRef = useRef<HTMLDivElement>(null)
  const favoriteRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [formValues, setFormValues] = useState<UiAirflowsSearchFilter>({
    autoRegenerateDag: autoRegenerateDag ? autoRegenerateDag : null
  })

  useFocusTrap(
    containerRef,
    isSearchFilterOpen,
    openDropdown === 'addFavoriteDropdown'
  )

  useEffect(() => {
    setAutoRegenerateDagFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.autoRegenerateDag
    }))
  }, [formValues])

  useEffect(() => {
    if (!isSearchFilterOpen) return

    const containerRefCurrent = containerRef.current

    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as Node

      if (containerRefCurrent && !containerRefCurrent.contains(target)) {
        onToggle(false)
        setOpenDropdown(null)
        return
      }

      if (
        openDropdown &&
        (openDropdown === 'addFavoriteDropdown' ||
          openDropdown === 'favoritesDropdown') &&
        favoriteRef.current &&
        !favoriteRef.current.contains(target)
      ) {
        setOpenDropdown(null)
      }
    }

    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (openDropdown) {
          setOpenDropdown(null)
        } else {
          onToggle(false)
        }
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keyup', handleKeyUp)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keyup', handleKeyUp)
    }
  }, [isSearchFilterOpen, onToggle, openDropdown])

  // const handleKeyDownOnInput = (
  //   event: React.KeyboardEvent<HTMLInputElement>
  // ) => {
  //   if (event.key === 'Enter') {
  //     buttonRef.current?.classList.add('active')
  //   }
  // }

  // const handleKeyUpOnInput = (event: React.KeyboardEvent<HTMLInputElement>) => {
  //   if (event.key === 'Enter') {
  //     buttonRef.current?.classList.remove('active')
  //   }
  // }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setTimeout(() => {
        setOpenDropdown(null)
      }, 0)
    }
  }

  const handleShow = () => {
    setOpenDropdown(null)
    onShow(formValues)
  }

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string
  ) => {
    if (!keyLabel) return

    if (keyLabel === 'autoRegenerateDag') {
      const value = typeof item === 'string' ? item : null

      setAutoRegenerateDagFilter((prev) => ({
        ...prev,
        value: value
      }))

      setFormValues((prev) => ({
        ...prev,
        autoRegenerateDag: item as string | null
      }))
    } else {
      setFormValues((prev) => ({
        ...prev,
        [keyLabel]: item
      }))
    }
  }

  return (
    <div className="search-filter-container" ref={containerRef}>
      <button
        className="search-filter-button"
        onClick={() => onToggle(!isSearchFilterOpen)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
            onToggle(!isSearchFilterOpen)
          }
        }}
        tabIndex={0}
        disabled={disabled}
      >
        <div className="search-filter-funnel">
          <FilterFunnel />
        </div>
        Filter
        <div className="search-filter-chevron-container">
          {isSearchFilterOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>
      {isSearchFilterOpen && (
        <div className="search-filter-dropdown" style={{ width: '232px' }}>
          <div className="search-filter-dropdown-h-ctn">
            <h3>Filter and show DAGs</h3>
            <FavoriteFilterSearch<UiAirflowsSearchFilter>
              ref={favoriteRef}
              type={`airflow-${type}`}
              formValues={formValues}
              onSelectFavorite={(favoriteState) => setFormValues(favoriteState)}
              openDropdown={openDropdown}
              handleDropdownToggle={handleDropdownToggle}
            />
          </div>

          {/* <p>{`Use the asterisk (*) as a wildcard character for partial matches.`}</p> */}
          <form
            onSubmit={(event) => {
              event.preventDefault()
              handleShow()
            }}
          >
            <div className="filter-container">
              <div className="filter-first-container">
                <label
                  htmlFor={`dropdown-autoRegenerateDag`}
                  key={`dropdown-autoRegenerateDag`}
                >
                  {autoRegenerateDagFilter.label}:
                  <Dropdown
                    id={`dropdown-autoRegenerateDag`}
                    keyLabel={autoRegenerateDagFilter.keyLabel}
                    items={autoRegenerateDagFilter.items}
                    onSelect={handleSelect}
                    isOpen={openDropdown === `dropdown-autoRegenerateDag`}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle(`dropdown-autoRegenerateDag`, isOpen)
                    }
                    searchFilter={false}
                    initialTitle={
                      autoRegenerateDagFilter.value
                        ? String(autoRegenerateDagFilter.value)
                        : 'Select...'
                    }
                    cross={true}
                    backgroundColor="inherit"
                    textColor="black"
                    fontSize="14px"
                    border="0.5px solid rgb(42, 42, 42)"
                    borderRadius="3px"
                    height="21.5px"
                    width="212px"
                    chevronWidth="11"
                    chevronHeight="7"
                    lightStyle={true}
                  />
                </label>
              </div>
            </div>
            <div
              className="submit-button-container"
              style={{ marginTop: '23px' }}
            >
              <Button type="submit" title="Show" ref={buttonRef} />
            </div>
          </form>
        </div>
      )}
    </div>
  )
}

export default AirflowSearchFilterDags
