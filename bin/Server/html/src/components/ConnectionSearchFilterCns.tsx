import { useEffect, useMemo, useRef, useState } from 'react'
import { useConnections } from '../utils/queries'
import { useAtom } from 'jotai'
import { isCnDropdownReadyAtom } from '../atoms/atoms'
import Dropdown from './Dropdown'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import FilterFunnel from '../assets/icons/FilterFunnel'
import './SearchFilterTables.scss'
import Button from './Button'
import { ConnectionSearchFilter } from '../utils/interfaces'
import FavoriteFilterSearch from './FavoriteFilterSearch'
import { useFocusTrap } from '../utils/hooks'

interface ExportSearchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (filters: ConnectionSearchFilter) => void
  disabled?: boolean
}

function ConnectionSearchFilterCns({
  isSearchFilterOpen,
  onToggle,
  onShow,
  disabled
}: ExportSearchFilterProps) {
  const query = new URLSearchParams(location.search)

  const name = query.get('name') || null
  const connectionString = query.get('connectionString') || null

  const [isCnDropdownReady, setIsCnDropdownReady] = useAtom(
    isCnDropdownReadyAtom
  )

  const { data, isLoading } = useConnections()
  const connectionNames = useMemo(() => {
    return Array.isArray(data) ? data.map((connection) => connection.name) : []
  }, [data])

  const containerRef = useRef<HTMLDivElement>(null)
  const favoriteRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [formValues, setFormValues] = useState<ConnectionSearchFilter>({
    name: name ? name : null,
    connectionString: connectionString ? connectionString : null
  })

  useFocusTrap(
    containerRef,
    isSearchFilterOpen,
    openDropdown === 'addFavoriteDropdown'
  )

  useEffect(() => {
    if (isLoading || !connectionNames.length) return
    setIsCnDropdownReady(true)
  }, [connectionNames.length, isLoading, setIsCnDropdownReady])

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

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (openDropdown) {
          setOpenDropdown(null)
        } else {
          onToggle(false)
        }
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
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

  const handleInputDropdownChange = (value: string | null) => {
    setFormValues((prev) => ({ ...prev, name: value }))

    if (value && value.length > 0) {
      const matches = connectionNames.some((name) =>
        name.toLowerCase().startsWith(value.toLowerCase())
      )
      if (matches) {
        setOpenDropdown('cnSearch')
      } else {
        setOpenDropdown(null)
      }
    } else {
      setOpenDropdown(null)
    }
  }

  const handleInputDropdownSelect = (item: string | null) => {
    setFormValues((prev) => ({ ...prev, name: item }))
  }

  const filteredConnectionNames = useMemo(() => {
    if (!formValues.name) return connectionNames
    return connectionNames.filter((name) =>
      name.toLowerCase().includes(formValues.name!.toLowerCase())
    )
  }, [formValues.name, connectionNames])

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
        <div className="search-filter-dropdown" style={{ width: '212px' }}>
          <div className="search-filter-dropdown-h-ctn">
            <h3>Search and show connections</h3>
            <FavoriteFilterSearch<ConnectionSearchFilter>
              ref={favoriteRef}
              type="connection"
              formValues={formValues}
              onSelectFavorite={(favoriteState) => setFormValues(favoriteState)}
              openDropdown={openDropdown}
              handleDropdownToggle={handleDropdownToggle}
            />
          </div>

          <p>{`Use the asterisk (*) as a wildcard character for partial matches.`}</p>
          <form
            onSubmit={(event) => {
              event.preventDefault()
              handleShow()
            }}
          >
            <div className="filter-container">
              <div className="filter-first-container">
                <label htmlFor="searchFilterConnection">
                  Name:
                  {isCnDropdownReady && (
                    <>
                      <input
                        id="searchFilterConnection"
                        type="text"
                        value={formValues.name || ''}
                        onChange={(event) =>
                          handleInputDropdownChange(event.target.value)
                        }
                        onKeyDown={(event) => {
                          if (event.key === 'Enter') {
                            event.preventDefault() // Prevents Enter from triggering form submission
                          }
                        }}
                        autoComplete="off"

                        // onKeyDown={handleKeyDownOnInput}
                        // onKeyUp={handleKeyUpOnInput}
                      />
                      <Dropdown
                        id="searchFilterConnection"
                        items={
                          filteredConnectionNames.length > 0
                            ? filteredConnectionNames
                            : ['No Connection yet']
                        }
                        onSelect={handleInputDropdownSelect}
                        isOpen={openDropdown === 'cnSearch'}
                        onToggle={(isOpen: boolean) =>
                          handleDropdownToggle('cnSearch', isOpen)
                        }
                        searchFilter={false}
                        textInputMode={true}
                        backgroundColor="inherit"
                        textColor="black"
                        border="0.5px solid rgb(42, 42, 42)"
                        borderRadius="3px"
                        height="21.5px"
                        lightStyle={true}
                      />
                    </>
                  )}
                </label>
                <label htmlFor="searchFilterTargetTable">
                  Connection string:
                  <input
                    id="searchFilterTargetTable"
                    type="text"
                    value={formValues.connectionString || ''}
                    onChange={(event) =>
                      setFormValues((prev) => ({
                        ...prev,
                        connectionString: event.target.value
                      }))
                    }
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"

                    // onKeyDown={handleKeyDownOnInput}
                    // onKeyUp={handleKeyUpOnInput}
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

export default ConnectionSearchFilterCns
