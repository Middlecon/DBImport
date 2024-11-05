import { Outlet, useNavigate, useParams } from 'react-router-dom'
import Dropdown from '../../components/Dropdown'
import { useDatabases, useDbTables } from '../../utils/queries'
import './Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import CreateImportTableModal from '../../components/CreateImportTableModal'
import { EditSetting } from '../../utils/interfaces'
import { createTableData } from '../../utils/dataFunctions'
import { useCreateImportTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useAtom } from 'jotai'
import {
  importDbListFiltersAtom,
  isDbDropdownReadyAtom,
  selectedImportDatabaseAtom
} from '../../atoms/atoms'

function Import() {
  const { data, isLoading } = useDatabases()

  const databaseNames = useMemo(() => {
    return Array.isArray(data) ? data.map((database) => database.name) : []
  }, [data])

  const navigate = useNavigate()
  const { database } = useParams<{ database: string }>()
  const { data: tables } = useDbTables(database ? database : null)
  const { mutate: createTable } = useCreateImportTable()
  const queryClient = useQueryClient()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)
  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )
  const [selectedDatabase, setSelectedDatabase] = useAtom(
    selectedImportDatabaseAtom
  )
  const [, setSelectedFilters] = useAtom(importDbListFiltersAtom)

  useEffect(() => {
    if (isLoading || !databaseNames.length) return

    if (database && databaseNames.includes(database)) {
      if (database !== selectedDatabase) {
        setSelectedDatabase(database)
      }
    } else {
      setSelectedDatabase(null)
      setSelectedFilters({})

      navigate('/import', { replace: true })
    }

    setIsDbDropdownReady(true)
  }, [
    database,
    databaseNames,
    isLoading,
    navigate,
    selectedDatabase,
    setIsDbDropdownReady,
    setSelectedDatabase,
    setSelectedFilters
  ])

  const handleSelect = (item: string | null) => {
    setSelectedFilters({})
    navigate(`/import/${item}`)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const mostCommonConnection = useMemo(() => {
    if (!tables || tables.length < 1) return null
    const connectionCounts = tables.reduce((acc, row) => {
      const connection = row.connection
      if (connection) {
        acc[connection] = (acc[connection] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    return Object.keys(connectionCounts).reduce((a, b) =>
      connectionCounts[a] > connectionCounts[b] ? a : b
    )
  }, [tables])

  const handleSave = (newTableData: EditSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['tables', newTable.database]
        })
        console.log('Create successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error creating table', error)
      }
    })
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Import</h1>
          <div className="db-dropdown">
            {selectedDatabase && (
              <Button
                title="+ Create table"
                onClick={() => setCreateModalOpen(true)}
                fontFamily={`'Work Sans Variable', sans-serif`}
                fontSize="14px"
                padding="4px 13px 7.5px 9px"
              />
            )}
            {isDbDropdownReady && (
              <Dropdown
                items={databaseNames.length > 0 ? databaseNames : ['No DB yet']}
                onSelect={handleSelect}
                isOpen={openDropdown === 'dbSearch'}
                onToggle={(isOpen: boolean) =>
                  handleDropdownToggle('dbSearch', isOpen)
                }
                searchFilter={true}
                initialTitle={selectedDatabase || 'Select DB'}
                leftwards={true}
                chevron={true}
                placeholder="Search for db..."
              />
            )}
          </div>
        </div>

        {!database ? (
          <div className="import-text-block">
            <p>
              Please select a DB in the above dropdown to show and edit settings
              for its tables.
            </p>
          </div>
        ) : (
          <>
            <Outlet />
            {isCreateModalOpen && mostCommonConnection && (
              <CreateImportTableModal
                database={database}
                prefilledConnection={mostCommonConnection}
                onSave={handleSave}
                onClose={() => setCreateModalOpen(false)}
              />
            )}
          </>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Import
