import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { generateDagSettings } from '../utils/cardRenderFormatting'
import { useGenerateDag } from '../utils/mutations'
import { ErrorData } from '../utils/interfaces'

interface GenerateDagModalProps {
  dagName: string
  isGenDagModalOpen: boolean
  onClose: () => void
}

function GenerateDagModal({
  dagName,
  isGenDagModalOpen,
  onClose
}: GenerateDagModalProps) {
  const [isGenDagLoading, setIsGenDagLoading] = useState(false)
  const [errorMessageGenerate, setErrorMessageGenerate] = useState<
    string | null
  >(null)
  const [successMessageGenerate, setSuccessMessageGenerate] = useState<
    string | null
  >(null)

  const { mutate: generateDag } = useGenerateDag()

  const settings = generateDagSettings(dagName ? dagName : '')

  const handleGenerateDag = () => {
    if (!dagName) {
      console.log('No selected row or DAG name', dagName)
      return
    }

    setIsGenDagLoading(true)

    generateDag(dagName, {
      onSuccess: (response) => {
        setIsGenDagLoading(false)
        setSuccessMessageGenerate('Generate DAG succeeded')
        console.log('Generating DAG succeeded, result:', response)
        // setIsGenDagModalOpen(false)
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.statusText &&
          error.response?.data.detail !== undefined
            ? `${error.message} ${error.response?.statusText}, ${error.response?.data.detail[0].msg}: ${error.response?.data.detail[0].type}`
            : error.status === 500
            ? `${error.message} ${error.response?.statusText}: ${error.response?.data}`
            : 'An unknown error occurred'
        setIsGenDagLoading(false)
        setErrorMessageGenerate(errorMessage)
        console.log('error', error)
        console.error('Generate DAG failed', error.message)
      }
    })
  }

  return (
    <EditTableModal
      isEditModalOpen={isGenDagModalOpen}
      title={`Generate DAG`}
      settings={settings}
      onSave={handleGenerateDag}
      onClose={() => {
        onClose()
        setErrorMessageGenerate(null)
        setSuccessMessageGenerate(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isGenDagLoading}
      loadingText="Generating"
      successMessage={successMessageGenerate}
      errorMessage={errorMessageGenerate ? errorMessageGenerate : null}
      onResetErrorMessage={() => setErrorMessageGenerate(null)}
      submitButtonTitle="Generate"
      closeButtonTitle="Close"
    />
  )
}

export default GenerateDagModal
