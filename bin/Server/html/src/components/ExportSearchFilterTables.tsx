import { useEffect, useMemo, useRef, useState } from 'react'
import { useExportConnections } from '../utils/queries'
import { useAtom } from 'jotai'
import { isDbDropdownReadyAtom } from '../atoms/atoms'
import Dropdown from './Dropdown'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import FilterFunnel from '../assets/icons/FilterFunnel'
import './SearchFilterTables.scss'
import Button from './Button'

interface ExportSearchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (
    connection: string | null,
    targetTable: string | null,
    targetSchema: string | null
  ) => void
}

function ExportSearchFilterTables({
  isSearchFilterOpen,
  onToggle,
  onShow
}: ExportSearchFilterProps) {
  const query = new URLSearchParams(location.search)
  const connection = query.get('connection') || null
  const targetTable = query.get('targetTable') || null
  const targetSchema = query.get('targetSchema') || null

  const { data, isLoading } = useExportConnections()
  const cnNames = useMemo(() => {
    return Array.isArray(data) ? data.map((connection) => connection.name) : []
  }, [data])
  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )

  const containerRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [formValues, setFormValues] = useState<{
    connection: string | null
    targetTable: string | null
    targetSchema: string | null
  }>({
    connection: connection ? connection : null,
    targetTable: targetTable ? targetTable : null,
    targetSchema: targetSchema ? targetSchema : null
  })

  useEffect(() => {
    if (isLoading || !cnNames.length) return
    setIsDbDropdownReady(true)
  }, [cnNames.length, isLoading, setIsDbDropdownReady])

  useEffect(() => {
    if (!isSearchFilterOpen) return

    const handleClickOutside = (event: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(event.target as Node)
      ) {
        onToggle(false)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (openDropdown === 'cnSearch') {
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
      setOpenDropdown(null)
    }
  }

  const handleShow = () => {
    onShow(
      formValues.connection ? `${formValues.connection}` : null,
      formValues.targetTable ? `${formValues.targetTable}` : null,
      formValues.targetSchema ? `${formValues.targetSchema}` : null
    )
  }

  const handleInputChange = (value: string) => {
    setFormValues((prev) => ({ ...prev, connection: value }))

    if (value.length > 0) {
      const matches = cnNames.some((name) =>
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

  const handleDropdownSelect = (item: string | null) => {
    setFormValues((prev) => ({ ...prev, connection: item }))
  }

  const filteredConnectionNames = useMemo(() => {
    if (!formValues.connection) return cnNames
    return cnNames.filter((name) =>
      name.toLowerCase().includes(formValues.connection!.toLowerCase())
    )
  }, [formValues.connection, cnNames])

  return (
    <div className="search-filter-container" ref={containerRef}>
      <div
        className="search-filter-button"
        onClick={() => onToggle(!isSearchFilterOpen)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
            onToggle(!isSearchFilterOpen)
          }
        }}
        tabIndex={0}
      >
        <div className="search-filter-funnel">
          <FilterFunnel />
        </div>
        Filter
        <div className="chevron-container">
          {isSearchFilterOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </div>
      {isSearchFilterOpen && (
        <div className="search-filter-dropdown">
          <h3>Search and show tables</h3>
          <p>{`Use the asterisk (*) as a wildcard character for partial matches.`}</p>
          <form
            onSubmit={(event) => {
              event.preventDefault()
              handleShow()
            }}
          >
            <label htmlFor="searchFilterConnection">
              Connection:
              {isDbDropdownReady && (
                <>
                  <input
                    id="searchFilterConnection"
                    type="text"
                    value={formValues.connection || ''}
                    onChange={(event) => handleInputChange(event.target.value)}
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
                    onSelect={handleDropdownSelect}
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
                    padding="8px 4px"
                    lightStyle={true}
                  />
                </>
              )}
            </label>
            <label htmlFor="searchFilterTargetTable">
              Target Table:
              <input
                id="searchFilterTargetTable"
                type="text"
                value={formValues.targetTable || ''}
                onChange={(event) =>
                  setFormValues((prev) => ({
                    ...prev,
                    targetTable: event.target.value
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
            <label htmlFor="searchFilterTargetSchema">
              Target Schema:
              <input
                id="searchFilterTargetSchema"
                type="text"
                value={formValues.targetSchema || ''}
                onChange={(event) =>
                  setFormValues((prev) => ({
                    ...prev,
                    targetSchema: event.target.value
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
            <div className="submit-button-container">
              <Button type="submit" title="Show" ref={buttonRef} />
            </div>
          </form>
        </div>
      )}
    </div>
  )
}

export default ExportSearchFilterTables
