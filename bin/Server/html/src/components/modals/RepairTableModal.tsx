import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { repairTableSettings } from '../../utils/cardRenderFormatting'
import { useRepairTable } from '../../utils/mutations'
import { ErrorData, ExportPKs, ImportPKs } from '../../utils/interfaces'
import infoTexts from '../../infoTexts.json'

interface RepairTableModalProps {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
  isRepairTableModalOpen: boolean
  onClose: () => void
}

function RepairTableModal({
  type,
  primaryKeys,
  isRepairTableModalOpen,
  onClose
}: RepairTableModalProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const { mutate: repairTable } = useRepairTable()

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

  const settings = repairTableSettings(label, tableName)

  const handleRepairTable = () => {
    if (!tableName) {
      console.log('No selected row or table name', tableName)
      return
    }

    setIsLoading(true)

    repairTable(
      { type, primaryKeys },
      {
        onSuccess: (response) => {
          setIsLoading(false)
          setSuccessMessage('Repair table succeeded')
          console.log('Repair table succeeded, result:', response)
        },
        onError: (error: AxiosError<ErrorData>) => {
          const errorMessage =
            error.response?.data?.result || 'An unknown error occurred'
          setIsLoading(false)
          setErrorMessage(errorMessage)
          console.log('error', error)
          console.error('Repair table failed', error.message)
        }
      }
    )
  }

  return (
    <EditTableModal
      isEditModalOpen={isRepairTableModalOpen}
      title={`Repair table`}
      titleInfoText={infoTexts.actions.repairTable}
      settings={settings}
      onSave={handleRepairTable}
      onClose={() => {
        onClose()
        setErrorMessage(null)
        setSuccessMessage(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isLoading}
      loadingText="Repairing"
      successMessage={successMessage}
      errorMessage={errorMessage ? errorMessage : null}
      onResetErrorMessage={() => setErrorMessage(null)}
      submitButtonTitle="Repair"
      closeButtonTitle="Close"
    />
  )
}

export default RepairTableModal
