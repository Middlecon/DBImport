import '../import/Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useSearchConnections } from '../../utils/queries'
import {
  Column,
  Connections,
  ConnectionSearchFilter,
  EditSetting
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
import DropdownCheckbox from '../../components/DropdownCheckbox'
import {
  connectionFilterAtom,
  connectionPersistStateAtom
} from '../../atoms/atoms'
import { useAtom } from 'jotai'
import { useLocation, useNavigate } from 'react-router-dom'
import ConnectionSearchFilterCns from '../../components/ConnectionSearchFilterCns'
import ListRowsInfo from '../../components/ListRowsInfo'
import ConfirmationModal from '../../components/ConfirmationModal'
import { useQueryClient } from '@tanstack/react-query'
import {
  useDeleteConnection,
  useCreateOrUpdateConnection
} from '../../utils/mutations'
import CreateConnectionModal from '../../components/CreateConnectionModal'
import Button from '../../components/Button'
import { createConnectionData } from '../../utils/dataFunctions'

const checkboxFilters = [
  {
    title: 'Server Type',
    accessor: 'serverType',
    values: [
      'MySQL',
      'Oracle',
      'MSSQL Server', // motsvarar SQL Server
      'PostgreSQL',
      'Progress DB', // motsvarar Progress
      'DB2 UDB',
      'DB2 AS400',
      'MongoDB',
      'CacheDB', // motsvarar Cache
      'Snowflake',
      'AWS S3',
      'Informix',
      'SQL Anywhere'
    ]
  }
]

function Connection() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['name', 'connectionString']
  const allParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/import', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allParams, navigate])

  const name = query.get('name') || null
  const connectionString = query.get('connectionString') || null

  const filters: ConnectionSearchFilter = useMemo(
    () => ({
      name,
      connectionString
    }),
    [name, connectionString]
  )

  const { data, isLoading: isSearchLoading } = useSearchConnections(filters)
  const { mutate: createConnection } = useCreateOrUpdateConnection()
  const { mutate: deleteConnection } = useDeleteConnection()
  const queryClient = useQueryClient()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentDeleteRow, setCurrentDeleteRow] = useState<Connections>()
  const [rowSelection, setRowSelection] = useState({})

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [, setConnectionPersistState] = useAtom(connectionPersistStateAtom)
  const [selectedFilters, setSelectedFilters] = useAtom(connectionFilterAtom)

  const handleShow = (uiFilters: ConnectionSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof ConnectionSearchFilter)[] = [
      'name',
      'connectionString'
    ]

    filterKeys.forEach((key) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(key, String(value))
      } else {
        params.delete(key)
      }
    })

    const orderedSearch = filterKeys
      .map((key) =>
        params.has(key) ? `${key}=${params.get(key) || ''}` : null
      )
      .filter((param) => param !== null)
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setConnectionPersistState(`/connection?${orderedSearch}`)
      navigate(`/connection?${orderedSearch}`, { replace: true })
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleSelect = (filterKey: string, items: string[]) => {
    setSelectedFilters((prevFilters) => ({
      ...prevFilters,
      [filterKey]: items
    }))
  }

  const handleSave = (newConnectionSettings: EditSetting[]) => {
    const newConnectionData = createConnectionData(newConnectionSettings)
    console.log('newConnectionData', newConnectionData)

    createConnection(newConnectionData, {
      onSuccess: (response) => {
        // For getting fresh data from database to the cache
        queryClient.invalidateQueries({
          queryKey: ['connection', 'search', filters]
        })
        queryClient.invalidateQueries({
          queryKey: ['connection']
        })
        console.log('Update successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error updating connection', error)
      }
    })
  }

  const handleDeleteIconClick = (row: Connections) => {
    setShowDeleteConfirmation(true)
    setCurrentDeleteRow(row)
  }

  const handleDelete = async (row: Connections) => {
    setShowDeleteConfirmation(false)

    const { name: connectionDelete } = row

    deleteConnection(
      { connectionName: connectionDelete },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['connection'], // Matches all related queries that starts the queryKey with 'connection'
            exact: false
          })
          console.log('Delete successful')
        },
        onError: (error) => {
          console.error('Error deleting item', error)
        }
      }
    )
  }

  const columns: Column<Connections>[] = useMemo(
    () => [
      { header: 'Name', accessor: 'name' },
      { header: 'Server Type', accessor: 'serverType' },
      { header: 'Connection string', accessor: 'connectionString' },
      { header: 'Links', isLink: 'connectionLink' },
      { header: 'Actions', isAction: 'delete' }
    ],
    []
  )

  const filteredData = useMemo(() => {
    console.log('data', data)
    if (!data || !Array.isArray(data.connections)) return []
    return data.connections.filter((row) => {
      return [...checkboxFilters].every((filter) => {
        const selectedItems = Array.isArray(selectedFilters[filter.accessor])
          ? selectedFilters[filter.accessor]?.map((value) => value)
          : []

        if (selectedItems.length === 0) return true

        const accessorKey = filter.accessor as keyof typeof row
        const displayKey = `${String(accessorKey)}Display` as keyof typeof row
        const rowValue = (row[displayKey] ?? row[accessorKey]) as string

        return selectedItems.includes(rowValue)
      })
    })
  }, [data, selectedFilters])
  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Connection</h1>
          <div className="db-dropdown">
            <Button
              title="+ Create"
              onClick={() => setCreateModalOpen(true)}
              fontFamily={`'Work Sans Variable', sans-serif`}
              fontSize="14px"
            />

            <ConnectionSearchFilterCns
              isSearchFilterOpen={openDropdown === 'searchFilter'}
              onToggle={(isSearchFilterOpen: boolean) =>
                handleDropdownToggle('searchFilter', isSearchFilterOpen)
              }
              onShow={handleShow}
              disabled={!data}
            />
          </div>
        </div>

        <div className="filters">
          {Array.isArray(checkboxFilters) &&
            checkboxFilters.map((filter, index) => (
              <DropdownCheckbox
                key={index}
                items={filter.values || []}
                title={filter.title}
                selectedItems={selectedFilters[filter.accessor] || []}
                onSelect={(items) => handleSelect(filter.accessor, items)}
                isOpen={openDropdown === filter.accessor}
                onToggle={(isOpen) =>
                  handleDropdownToggle(filter.accessor, isOpen)
                }
              />
            ))}
        </div>
        {data && Array.isArray(data.connections) ? (
          <>
            <div style={{ height: 27 }} />
            <ListRowsInfo
              filteredData={filteredData}
              contentTotalRows={data.headersRowInfo.contentTotalRows}
              contentMaxReturnedRows={
                data.headersRowInfo.contentMaxReturnedRows
              }
              itemType="connection"
            />
            {filteredData && (
              <TableList
                columns={columns}
                data={filteredData}
                isLoading={isSearchLoading}
                onDelete={handleDeleteIconClick}
                rowSelection={rowSelection}
                onRowSelectionChange={setRowSelection}
                enableMultiSelection={false}
              />
            )}
          </>
        ) : isSearchLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="import-text-block">
            <p>No connections yet.</p>
          </div>
        )}
        {isCreateModalOpen && (
          <CreateConnectionModal
            isCreateModalOpen={isCreateModalOpen}
            onSave={handleSave}
            onClose={() => setCreateModalOpen(false)}
          />
        )}
        {showDeleteConfirmation && currentDeleteRow && (
          <ConfirmationModal
            title={`Delete ${currentDeleteRow.name}`}
            message={`Are you sure that you want to delete connection "${currentDeleteRow.name}"? \nDelete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleDelete(currentDeleteRow)}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Connection
