import { Outlet, useNavigate, useParams } from 'react-router-dom'
import DropdownFiltered from '../../components/DropdownFiltered'
import { useDatabases } from '../../utils/queries'
import './Import.scss'
import { useEffect, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

function Import() {
  const { data, isLoading } = useDatabases()
  const databaseNames = useMemo(() => data?.map((db) => db.name) ?? [], [data])
  const navigate = useNavigate()
  const { db } = useParams<{ db: string }>()
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null)
  const queryClient = useQueryClient()

  console.log('databases data', data)
  console.log('databaseNames:', databaseNames)
  console.log('db param:', db)

  useEffect(() => {
    if (isLoading) return
    if (db && databaseNames.includes(db)) {
      setSelectedDatabase(db)
    } else if (db) {
      navigate('/import', { replace: true })
    }
  }, [db, databaseNames, isLoading, navigate])

  const handleSelect = (item: string) => {
    console.log('Selected item:', item)

    queryClient.invalidateQueries({ queryKey: ['tables'] })

    navigate(`/import/${item}`)
  }

  console.log('databases names', databaseNames)

  return (
    <>
      <div className="import-root">
        <h1>Import</h1>
        <div className="db-dropdown">
          <DropdownFiltered
            items={databaseNames.length > 0 ? databaseNames : ['No DB yet']}
            initialTitle={selectedDatabase || 'Select DB'}
            onSelect={handleSelect}
          />
        </div>
        {!db ? (
          <div className="import-text-block">
            <p>
              Please select a DB in the above dropdown to show and edit settings
              for its tables.
            </p>
          </div>
        ) : (
          <Outlet />
        )}
      </div>
    </>
  )
}

export default Import
