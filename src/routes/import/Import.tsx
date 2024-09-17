import { Outlet, useNavigate, useParams } from 'react-router-dom'
import DropdownSearch from '../../components/DropdownSearch'
import { useDatabases } from '../../utils/queries'
import './Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'

function Import() {
  const { data, isLoading } = useDatabases()
  // console.log('data DATABASES', data)

  const databaseNames = useMemo(() => data?.map((db) => db.name) ?? [], [data])
  const navigate = useNavigate()
  const { db } = useParams<{ db: string }>()
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  useEffect(() => {
    if (isLoading) return
    if (db && databaseNames.includes(db)) {
      setSelectedDatabase(db)
    } else if (db) {
      navigate('/import', { replace: true })
    }
  }, [db, databaseNames, isLoading, navigate])

  const handleSelect = (item: string) => {
    navigate(`/import/${item}`)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const outletContext = {
    openDropdown,
    handleDropdownToggle
  }

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import']}>
        <div className="import-header">
          <h1>Import</h1>
          <div className="db-dropdown">
            <DropdownSearch
              items={databaseNames.length > 0 ? databaseNames : ['No DB yet']}
              initialTitle={selectedDatabase || 'Select DB'}
              leftwards={true}
              onSelect={handleSelect}
              isOpen={openDropdown === 'dbSearch'}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle('dbSearch', isOpen)
              }
            />
          </div>
        </div>

        {!db ? (
          <div className="import-text-block">
            <p>
              Please select a DB in the above dropdown to show and edit settings
              for its tables.
            </p>
          </div>
        ) : (
          <Outlet context={outletContext} />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Import
