import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { tableNameReadonlySettings } from '../../utils/cardRenderFormatting'
import { useResetTable } from '../../utils/mutations'
import { ErrorData, ExportPKs, ImportPKs } from '../../utils/interfaces'
import infoTexts from '../../infoTexts.json'

interface ResetTableModalProps {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
  isResetTableModalOpen: boolean
  onClose: () => void
}

function ResetTableModal({
  type,
  primaryKeys,
  isResetTableModalOpen,
  onClose
}: ResetTableModalProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const { mutate: resetTable } = useResetTable()

  function isImportPKs(
    primaryKeys: ImportPKs | ExportPKs,
    type: 'import' | 'export'
  ): primaryKeys is ImportPKs {
    return type === 'import'
  }

  const tableName = isImportPKs(primaryKeys, type)
    ? primaryKeys.table // Narrowed to ImportPKs
    : primaryKeys.targetTable

  const label: string = type === 'import' ? 'Table' : 'Target Table'

  const settings = tableNameReadonlySettings(label, tableName)

  const handleResetTable = () => {
    if (!tableName) {
      console.log('No selected row or table name', tableName)
      return
    }

    setIsLoading(true)

    resetTable(
      { type, primaryKeys },
      {
        onSuccess: (response) => {
          setIsLoading(false)
          setSuccessMessage('Reset table succeeded')
          console.log('Reset table succeeded, result:', response)
        },
        onError: (error: AxiosError<ErrorData>) => {
          const errorMessage =
            error.response?.data?.result || 'An unknown error occurred'
          setIsLoading(false)
          setErrorMessage(errorMessage)
          console.log('error', error)
          console.error('Reset table failed', error.message)
        }
      }
    )
  }

  return (
    <EditTableModal
      isEditModalOpen={isResetTableModalOpen}
      title={`Reset table`}
      titleInfoText={infoTexts.actions.resetTable}
      settings={settings}
      onSave={handleResetTable}
      onClose={() => {
        onClose()
        setErrorMessage(null)
        setSuccessMessage(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isLoading}
      loadingText="Reseting"
      successMessage={successMessage}
      errorMessage={errorMessage ? errorMessage : null}
      onResetErrorMessage={() => setErrorMessage(null)}
      submitButtonTitle="Reset"
      closeButtonTitle="Close"
    />
  )
}

export default ResetTableModal
