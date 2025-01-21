import '../import/Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useSearchConnections } from '../../utils/queries'
import {
  Column,
  Connections,
  ConnectionSearchFilter,
  EditSetting,
  ErrorData
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
// import DropdownCheckbox from '../../components/DropdownCheckbox'

import { useLocation, useNavigate } from 'react-router-dom'
import ListRowsInfo from '../../components/ListRowsInfo'
import { useQueryClient } from '@tanstack/react-query'
import {
  useDeleteConnection,
  useEncryptCredentials,
  useTestConnection
} from '../../utils/mutations'
import ConnectionActions from './ConnectionActions'
import ConfirmationModal from '../../components/modals/ConfirmationModal'
import { AxiosError } from 'axios'
import EditTableModal from '../../components/modals/EditTableModal'
import { encryptCredentialsSettings } from '../../utils/cardRenderFormatting'
import { transformEncryptCredentialsSettings } from '../../utils/dataFunctions'

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
  const allSearchParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allSearchParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/connection', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allSearchParams, navigate])

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

  const [connectionParam, setConnectionParam] = useState<string | null>(null)

  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [selectedDeleteRow, setSelectedDeleteRow] = useState<Connections>()
  const [rowSelection, setRowSelection] = useState({}) // Not used practically at the moment

  const settings = encryptCredentialsSettings(
    connectionParam ? connectionParam : ''
  )

  const [isEncryptModalOpen, setIsEncryptModalOpen] = useState(false)
  const [isEncryptLoading, setIsEncryptLoading] = useState(false)
  const [errorMessageEncrypt, setErrorMessageEncrypt] = useState<string | null>(
    null
  )
  const { mutate: encryptCredentials } = useEncryptCredentials()

  const [isTestCnModalOpen, setIsTestCnModalOpen] = useState(false)
  const [isTestLoading, setIsTestLoading] = useState(true)
  const [errorMessageTest, setErrorMessageTest] = useState<string | null>(null)
  const { mutate: testConnection, isSuccess: isTestSuccess } =
    useTestConnection()

  // const [selectedFilters, setSelectedFilters] = useAtom(connectionFilterAtom)

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

  const handleEncryptIconClick = (row: Connections) => {
    setConnectionParam(row.name)
    setIsEncryptModalOpen(true)
  }

  const handleDeleteIconClick = (row: Connections) => {
    setShowDeleteConfirmation(true)
    setSelectedDeleteRow(row)
  }

  const handleTestConnection = (row: Connections) => {
    setConnectionParam(row.name)

    setIsTestCnModalOpen(true)

    testConnection(row.name, {
      onSuccess: (response) => {
        setIsTestLoading(false)
        console.log('Connection test succeeded, result:', response)
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.data?.result || 'An unknown error occurred'
        setIsTestLoading(false)
        setErrorMessageTest(errorMessage)
        setIsTestCnModalOpen(true)
        console.log('error', error)
        console.error('Connection test failed', error.message)
      }
    })
  }

  const handleSaveEncrypt = (updatedSettings: EditSetting[]) => {
    setIsEncryptLoading(true)
    console.log('updatedSettings', updatedSettings)
    const encryptCredentialsData =
      transformEncryptCredentialsSettings(updatedSettings)
    console.log('encryptCredentialsData', encryptCredentialsData)

    encryptCredentials(encryptCredentialsData, {
      onSuccess: (response) => {
        // For getting fresh data from database to the cache
        queryClient.invalidateQueries({
          queryKey: ['connection'],
          exact: false
        })
        console.log('Update successful', response)
        setIsEncryptLoading(false)
        setIsEncryptModalOpen(false)
      },
      onError: (error: AxiosError<ErrorData>) => {
        console.error('Error encrypt credentials', error)
        const errorMessage =
          error.response?.data?.result || 'An unknown error occurred'
        setErrorMessageEncrypt(errorMessage)
        setIsEncryptLoading(false)
      }
    })
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
          setRowSelection({})
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
      { header: 'Actions', isAction: 'testAndEncryptAndDelete' }
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
                onTestConnection={handleTestConnection}
                onEncryptCredentials={handleEncryptIconClick}
                onDelete={handleDeleteIconClick}
                rowSelection={rowSelection}
                onRowSelectionChange={setRowSelection}
                enableMultiSelection={false}
                isTwoLinks={true}
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

        {showDeleteConfirmation && selectedDeleteRow && (
          <ConfirmationModal
            title={`Delete ${selectedDeleteRow.name}`}
            message={`Are you sure that you want to delete connection "${selectedDeleteRow.name}"? \nDelete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={() => handleDelete(selectedDeleteRow)}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
        {isEncryptModalOpen && connectionParam && (
          <EditTableModal
            isEditModalOpen={isEncryptModalOpen}
            title={`Encrypt credentials`}
            settings={settings}
            onSave={handleSaveEncrypt}
            onClose={() => setIsEncryptModalOpen(false)}
            isNoCloseOnSave={true}
            initWidth={400}
            isLoading={isEncryptLoading}
            loadingText="Encrypting"
            errorMessage={errorMessageEncrypt ? errorMessageEncrypt : null}
            onResetErrorMessage={() => setErrorMessageEncrypt(null)}
            submitButtonTitle="Encrypt"
          />
        )}
        {isTestCnModalOpen && connectionParam && (
          <ConfirmationModal
            title={
              isTestLoading
                ? 'Testing'
                : `Test ${isTestSuccess ? 'successful' : 'failed'}`
            }
            message={
              isTestLoading
                ? ''
                : `Contact with connection ${connectionParam} ${
                    isTestSuccess ? 'verified' : 'failed'
                  }.`
            }
            isLoading={isTestLoading}
            errorInfo={isTestSuccess ? null : errorMessageTest}
            buttonTitleCancel="Close"
            onCancel={() => {
              setIsTestLoading(true)
              setIsTestCnModalOpen(false)
            }}
            isActive={isTestCnModalOpen}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Connection
