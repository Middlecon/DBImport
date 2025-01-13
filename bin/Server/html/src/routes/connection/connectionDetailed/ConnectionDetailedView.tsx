import { useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../_shared/TableDetailedView.scss'
import '../../import/tableDetailed/settings/TableSettings.scss'
import '../../../components/Loading.scss'
import ConnectionSettings from './ConnectionSettings'
import { useState } from 'react'
import DropdownActions from '../../../components/DropdownActions'
import TestConnectionIcon from '../../../assets/icons/TestConnectionIcon'
import EncryptIcon from '../../../assets/icons/EncryptIcon'
import EditTableModal from '../../../components/EditTableModal'
import {
  // encryptCredentialsSettings,
  encryptCredentialsSettings2
} from '../../../utils/cardRenderFormatting'
import { EditSetting } from '../../../utils/interfaces'
import { transformEncryptCredentialsSettings } from '../../../utils/dataFunctions'
import {
  useEncryptCredentials,
  useTestConnection
} from '../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import ConfirmationModal from '../../../components/ConfirmationModal'

function ConnectionDetailedView() {
  const queryClient = useQueryClient()
  const { mutate: encryptCredentials } = useEncryptCredentials()

  const { connection: connectionParam } = useParams<{
    connection: string
  }>()

  // const settings = encryptCredentialsSettings

  const settings2 = encryptCredentialsSettings2(
    connectionParam ? connectionParam : ''
  )

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [isEncryptModalOpen, setIsEncryptModalOpen] = useState(false)
  const [isTestCnModalOpen, setIsTestCnModalOpen] = useState(false)

  const { mutate: testConnection, isSuccess: isTestSuccess } =
    useTestConnection()

  const handleTestConnection = () => {
    if (!connectionParam) {
      console.log('No connectionParam', connectionParam)
      return
    }
    testConnection(connectionParam, {
      onSuccess: (response) => {
        setIsTestCnModalOpen(true)
        console.log('Connection test succeeded, result:', response)
      },
      onError: (error) => {
        setIsTestCnModalOpen(true)
        console.error('Connection test failed', error)
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
        setIsEncryptModalOpen(false)
      },
      onError: (error) => {
        console.error('Error updating table', error)
      }
    })
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>{`${connectionParam}`}</h1>
          <div className="actions-dropdown-container">
            <DropdownActions
              isDropdownActionsOpen={openDropdown === 'dropdownActions'}
              onToggle={(isDropdownActionsOpen: boolean) =>
                handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
              }
              items={[
                {
                  icon: <EncryptIcon />,
                  label: `Encrypt credentials`,
                  onClick: () => {
                    setIsEncryptModalOpen(true)
                    setOpenDropdown(null)
                  }
                },
                {
                  icon: <TestConnectionIcon />,
                  label: 'Test connection',
                  onClick: handleTestConnection
                }
              ]}
              disabled={!connectionParam}
            />
          </div>
        </div>
        <ConnectionSettings />
        {isEncryptModalOpen && connectionParam && (
          <EditTableModal
            isEditModalOpen={isEncryptModalOpen}
            // title={`Encrypt credentials for ${connectionParam}`}
            title={`Encrypt credentials`}
            settings={settings2}
            onSave={handleSave}
            onClose={() => setIsEncryptModalOpen(false)}
            initWidth={400}
          />
        )}
        {isTestCnModalOpen && (
          <ConfirmationModal
            title={`Test ${isTestSuccess ? 'successful' : 'failed'}`}
            message={`Contact with connection ${connectionParam} ${
              isTestSuccess ? 'verified' : 'failed'
            }.`}
            buttonTitleCancel="Close"
            onCancel={() => {
              setIsTestCnModalOpen(false)
            }}
            isActive={isTestCnModalOpen}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
