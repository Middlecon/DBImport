import { Outlet, useNavigate, useParams } from 'react-router-dom'
import Dropdown from '../../components/Dropdown'
import { useDatabases, useDbTables } from '../../utils/queries'
import './Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import CreateTableModal from '../../components/CreateTableModal'
import { TableSetting } from '../../utils/interfaces'
import { createTableData } from '../../utils/dataFunctions'
import { useCreateTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'

function Import() {
  const { data, isLoading } = useDatabases()
  // console.log('data DATABASES', data)

  const databaseNames = useMemo(
    () => data?.map((database) => database.name) ?? [],
    [data]
  )

  const navigate = useNavigate()
  const { database } = useParams<{ database: string }>()
  const { data: tables } = useDbTables(database ? database : null)
  const { mutate: createTable } = useCreateTable()
  const queryClient = useQueryClient()
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  useEffect(() => {
    if (isLoading) return

    if (!database || !databaseNames.includes(database)) {
      setSelectedDatabase(null)
    } else {
      setSelectedDatabase(database)
    }
  }, [database, databaseNames, isLoading, navigate])

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

  const mostCommonConnection = useMemo(() => {
    if (!tables || tables.length < 1) return null
    console.log(' mostCommonConnection tables', tables)
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

  const handleSave = (newTableData: TableSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['tables', newTable.database]
        })
        console.log('Update successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error updating table', error)
      }
    })
  }

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import']}>
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
              <CreateTableModal
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
