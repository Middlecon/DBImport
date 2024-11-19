import { useEffect, useMemo, useRef, useState } from 'react'
import { useDatabases } from '../utils/queries'
import { useAtom } from 'jotai'
import { isDbDropdownReadyAtom } from '../atoms/atoms'
import Dropdown from './Dropdown'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import FilterFunnel from '../assets/icons/FilterFunnel'
import './ImportSearchFilterTables.scss'
import Button from './Button'

interface serchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (database: string | null, table: string | null) => void
}

function ImportSearchFilterTables({
  isSearchFilterOpen,
  onToggle,
  onShow
}: serchFilterProps) {
  const query = new URLSearchParams(location.search)
  const database = query.get('database') || null
  const table = query.get('table') || null

  const { data, isLoading } = useDatabases()

  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )

  const containerRef = useRef<HTMLDivElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [formValues, setFormValues] = useState<{
    database: string | null
    table: string | null
  }>({
    database: database ? database : null,
    table: table ? table : null
  })

  const databaseNames = useMemo(() => {
    return Array.isArray(data) ? data.map((database) => database.name) : []
  }, [data])

  useEffect(() => {
    if (isLoading || !databaseNames.length) return
    setIsDbDropdownReady(true)
  }, [databaseNames.length, isLoading, setIsDbDropdownReady])

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
        onToggle(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isSearchFilterOpen, onToggle])

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleShow = () => {
    onShow(
      formValues.database ? `${formValues.database}` : null,
      formValues.table ? `${formValues.table}` : null
    )
  }

  const handleInputChange = (value: string) => {
    setFormValues((prev) => ({ ...prev, database: value }))

    if (value.length > 0) {
      const matches = databaseNames.some((name) =>
        name.toLowerCase().startsWith(value.toLowerCase())
      )
      if (matches) {
        setOpenDropdown('dbSearch')
      } else {
        setOpenDropdown(null)
      }
    } else {
      setOpenDropdown(null)
    }
  }

  const handleDropdownSelect = (item: string | null) => {
    setFormValues((prev) => ({ ...prev, database: item }))
  }

  const filteredDatabaseNames = useMemo(() => {
    if (!formValues.database) return databaseNames
    return databaseNames.filter((name) =>
      name.toLowerCase().includes(formValues.database!.toLowerCase())
    )
  }, [formValues.database, databaseNames])

  return (
    <div className="import-search-filter-container" ref={containerRef}>
      <div
        className="search-filter-button"
        onClick={() => onToggle(!isSearchFilterOpen)}
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
        <div className="import-search-filter-dropdown">
          <h3>Search and show tables</h3>
          <p>{`Use the asterisk (*) as a wildcard character for partial matches.`}</p>
          <form
            onSubmit={(event) => {
              event.preventDefault()
              handleShow()
            }}
          >
            <label htmlFor="searchFilterDatabase">
              Database:
              {isDbDropdownReady && (
                <>
                  <input
                    id="searchFilterDatabase"
                    type="text"
                    value={formValues.database || ''}
                    onChange={(event) => handleInputChange(event.target.value)}
                  />
                  <Dropdown
                    id="searchFilterDatabase"
                    items={
                      filteredDatabaseNames.length > 0
                        ? filteredDatabaseNames
                        : ['No DB yet']
                    }
                    onSelect={handleDropdownSelect}
                    isOpen={openDropdown === 'dbSearch'}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle('dbSearch', isOpen)
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
                    placeholder="Search for db..."
                  />
                </>
              )}
            </label>
            <label htmlFor="searchFilterTable">
              Table:
              <input
                id="searchFilterTable"
                type="text"
                value={formValues.table || ''}
                onChange={(event) =>
                  setFormValues((prev) => ({
                    ...prev,
                    table: event.target.value
                  }))
                }
              />
            </label>
            <div className="submit-button-container">
              <Button type="submit" title="Show" />
            </div>
          </form>
        </div>
      )}
    </div>
  )
}

export default ImportSearchFilterTables
