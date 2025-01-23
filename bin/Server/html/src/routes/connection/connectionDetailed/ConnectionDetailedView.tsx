import { useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../_shared/tableDetailed/DetailedView.scss'
import '../../_shared/tableDetailed/settings/TableSettings.scss'
import '../../../components/Loading.scss'
import ConnectionSettings from './ConnectionSettings'
import { useState } from 'react'
import DropdownActions from '../../../components/DropdownActions'
import TestConnectionIcon from '../../../assets/icons/TestConnectionIcon'
import EncryptIcon from '../../../assets/icons/EncryptIcon'
import EditTableModal from '../../../components/modals/EditTableModal'
import { encryptCredentialsSettings } from '../../../utils/cardRenderFormatting'
import { EditSetting, ErrorData } from '../../../utils/interfaces'
import { transformEncryptCredentialsSettings } from '../../../utils/dataFunctions'
import {
  useDeleteConnection,
  useEncryptCredentials,
  useTestConnection
} from '../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { AxiosError } from 'axios'
import ConfirmationModal from '../../../components/modals/ConfirmationModal'
import Button from '../../../components/Button'
import ImportIconSmall from '../../../assets/icons/ImportIconSmall'
import ExportIconSmall from '../../../assets/icons/ExportIconSmall'
import DeleteIcon from '../../../assets/icons/DeleteIcon'

function ConnectionDetailedView() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { connection: connectionParam } = useParams<{
    connection: string
  }>()

  const settings = encryptCredentialsSettings(
    connectionParam ? connectionParam : ''
  )

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const { mutate: deleteConnection } = useDeleteConnection()
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)

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

  const handleTestConnection = () => {
    if (!connectionParam) {
      console.log('No connectionParam', connectionParam)
      return
    }

    setIsTestCnModalOpen(true)

    testConnection(connectionParam, {
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

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleSave = (updatedSettings: EditSetting[]) => {
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

  const handleDeleteIconClick = () => {
    if (!connectionParam) return

    setShowDeleteConfirmation(true)
  }

  const handleDeleteCn = async () => {
    if (!connectionParam) return

    setShowDeleteConfirmation(false)

    deleteConnection(
      { connectionName: connectionParam },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['connection', 'search'], // Matches all related queries that starts the queryKey with 'connection'
            exact: false
          })
          console.log('Delete successful')
          navigate(`/connection`, { replace: true })
        },
        onError: (error) => {
          console.error('Error deleting item', error)
        }
      }
    )
  }

  const handleLinkClick = (type: 'import' | 'export', item: string) => {
    const encodedItem = encodeURIComponent(item)

    if (type === 'import') {
      navigate(`/import?connection=${encodedItem}`)
    } else if (type === 'export') {
      navigate(`/export?connection=${encodedItem}`)
    }
  }

  return (
    <>
      <ViewBaseLayout>
        {connectionParam && (
          <div className="detailed-view-header">
            <h1>{`${connectionParam}`}</h1>
            <div className="detailed-view-header-buttons">
              <Button
                title="Imports"
                icon={<ImportIconSmall />}
                onClick={() => handleLinkClick('import', connectionParam)}
              />
              <Button
                title="Exports"
                icon={<ExportIconSmall />}
                onClick={() => handleLinkClick('export', connectionParam)}
              />
              <DropdownActions
                isDropdownActionsOpen={openDropdown === 'dropdownActions'}
                onToggle={(isDropdownActionsOpen: boolean) =>
                  handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
                }
                items={[
                  {
                    icon: <TestConnectionIcon />,
                    label: 'Test connection',
                    onClick: handleTestConnection
                  },
                  {
                    icon: <EncryptIcon />,
                    label: `Encrypt credentials`,
                    onClick: () => {
                      setIsEncryptModalOpen(true)
                      setOpenDropdown(null)
                    }
                  },
                  {
                    icon: <DeleteIcon />,
                    label: `Delete connection`,
                    onClick: handleDeleteIconClick
                  }
                ]}
                disabled={!connectionParam}
              />
            </div>
          </div>
        )}
        <ConnectionSettings />

        {isTestCnModalOpen && (
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
        {isEncryptModalOpen && connectionParam && (
          <EditTableModal
            isEditModalOpen={isEncryptModalOpen}
            title={`Encrypt credentials`}
            settings={settings}
            onSave={handleSave}
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
        {showDeleteConfirmation && connectionParam && (
          <ConfirmationModal
            title={`Delete ${connectionParam}`}
            message={`Are you sure that you want to delete connection "${connectionParam}"? \nDelete is irreversable.`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={handleDeleteCn}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
