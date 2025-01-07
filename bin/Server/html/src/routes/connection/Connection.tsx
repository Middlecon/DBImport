import '../import/Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useSearchConnections } from '../../utils/queries'
import {
  Column,
  Connections,
  ConnectionSearchFilter
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'

import { useLocation, useNavigate } from 'react-router-dom'
import ListRowsInfo from '../../components/ListRowsInfo'
import ConfirmationModal from '../../components/ConfirmationModal'
import { useQueryClient } from '@tanstack/react-query'
import { useDeleteConnection } from '../../utils/mutations'
import ConnectionActions from './ConnectionActions'

// const checkboxFilters = [
//   {
//     title: 'Server Type',
//     accessor: 'serverType',
//     values: [
//       'MySQL',
//       'Oracle',
//       'MSSQL Server', // motsvarar SQL Server
//       'PostgreSQL',
//       'Progress', // motsvarar Progress DB
//       'DB2 UDB',
//       'DB2 AS400',
//       'MongoDB',
//       'Cache', // motsvarar CacheDB
//       'Snowflake',
//       'AWS S3',
//       'Informix',
//       'SQL Anywhere'
//     ]
//   }
// ]

function Connection() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = ['name', 'connectionString', 'serverType']
  const allParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/connection', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allParams, navigate])

  const name = query.get('name') || null
  const connectionString = query.get('connectionString') || null
  const serverType = query.get('serverType') || null

  const filters: ConnectionSearchFilter = useMemo(
    () => ({
      name,
      connectionString
    }),
    [name, connectionString]
  )

  const { data, isLoading: isSearchLoading } = useSearchConnections(filters)

  const { mutate: deleteConnection } = useDeleteConnection()
  const queryClient = useQueryClient()

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [currentDeleteRow, setCurrentDeleteRow] = useState<Connections>()
  const [rowSelection, setRowSelection] = useState({})

  // const [selectedFilters, setSelectedFilters] = useAtom(connectionFilterAtom)

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

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
    if (!data || !Array.isArray(data.connections)) return []
    console.log('serverType', serverType)
    return data.connections.filter((row) => {
      if (serverType === null) return true

      return row.serverType === serverType
    })
  }, [data, serverType])

  return (
    <>
      <ViewBaseLayout>
        <div className="header-container" style={{ paddingBottom: 0 }}>
          <h1>Connection</h1>
          <ConnectionActions
            connections={data?.connections}
            filters={filters}
          />
        </div>

        {/* <div className="filters">
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
        </div> */}
        {data && Array.isArray(data.connections) && data.headersRowInfo ? (
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
          <div className="text-block">
            <p>No connections yet.</p>
          </div>
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
