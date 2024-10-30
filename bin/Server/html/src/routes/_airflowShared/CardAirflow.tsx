import { useState } from 'react'
import '../import/tableDetailed/settings/Card.scss'
import Button from '../../components/Button'
import Setting from '../import/tableDetailed/settings/Setting'
import {
  BaseAirflowDAG,
  CustomAirflowDAG,
  EditSetting,
  ExportAirflowDAG,
  ImportAirflowDAG,
  WithDynamicKeys
} from '../../utils/interfaces'
import EditConnectionModal from '../../components/EditConnectionModal'
import { useParams } from 'react-router-dom'
import {
  updateCustomDagData,
  updateExportDagData,
  updateImportDagData
} from '../../utils/dataFunctions'
import { useUpdateAirflowDag } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useAtom } from 'jotai'
import { airflowTypeAtom } from '../../atoms/atoms'

interface CardAirflowProps {
  type: 'import' | 'export' | 'custom'
  title: string
  settings: EditSetting[]
  originalData: ImportAirflowDAG | BaseAirflowDAG | ExportAirflowDAG
  isNotEditable?: boolean
  isDisabled?: boolean
}

function CardAirflow({
  type,
  title,
  settings,
  originalData,
  isNotEditable,
  isDisabled
}: CardAirflowProps) {
  const { dagName } = useParams<{
    dagName: string
  }>()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateDag } = useUpdateAirflowDag()
  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)
  const [, setAirflowType] = useAtom(airflowTypeAtom)
  setAirflowType(type)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const dagDataCopy = { ...originalData }

    let editedDagData:
      | ImportAirflowDAG
      | BaseAirflowDAG
      | ExportAirflowDAG
      | null = null
    if (type === 'import') {
      editedDagData = updateImportDagData(
        dagDataCopy as WithDynamicKeys<ImportAirflowDAG>,
        updatedSettings
      )
    } else if (type === 'export') {
      editedDagData = updateExportDagData(
        dagDataCopy as WithDynamicKeys<ExportAirflowDAG>,
        updatedSettings
      )
    } else if (type === 'custom') {
      editedDagData = updateCustomDagData(
        dagDataCopy as WithDynamicKeys<CustomAirflowDAG>,
        updatedSettings
      )
    }

    if (editedDagData && type) {
      queryClient.setQueryData(['airflows', type, dagName], editedDagData)
      updateDag(
        { type, dagData: editedDagData },
        {
          onSuccess: (response) => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', type, dagName]
            }) // For getting fresh data from database to the cache
            console.log('Update successful', response)
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(['airflows', type, dagName], originalData)

            console.error('Error updating table', error)
          }
        }
      )
    }
  }

  return (
    <div
      className={isDisabled ? 'card-disabled' : 'card'}
      style={{ width: 1000 }}
    >
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        {!isNotEditable && <Button title="Edit" onClick={handleOpenModal} />}
      </div>
      <dl className="card-dl">
        {Array.isArray(settings) &&
          settings.map((setting, index) => (
            <Setting key={index} {...setting} valueFieldWidth="739px" />
          ))}
      </dl>

      {isEditModalOpen && !isNotEditable && !isDisabled && (
        <EditConnectionModal
          title={`Edit ${title}`}
          settings={settings}
          onSave={handleSave}
          onClose={handleCloseModal}
        />
      )}
    </div>
  )
}

export default CardAirflow
