import { useState } from 'react'
import '../import/tableDetailed/settings/Card.scss'
import Button from '../../components/Button'
import Setting from '../import/tableDetailed/settings/Setting'
import { EditSetting, ImportAirflowDAG } from '../../utils/interfaces'
import EditConnectionModal from '../../components/EditConnectionModal'
// import { useUpdateTable } from '../../../../utils/mutations'
// import { useQueryClient } from '@tanstack/react-query'
// import { useParams } from 'react-router-dom'
// import { updateTableData } from '../../../../utils/dataFunctions'

interface CardAirflowProps {
  title: string
  settings: EditSetting[]
  originalData: ImportAirflowDAG
  isNotEditable?: boolean
  isDisabled?: boolean
}

function CardAirflow({
  title,
  settings,
  originalData,
  isNotEditable,
  isDisabled
}: CardAirflowProps) {
  // const { table: tableParam } = useParams<{ table: string }>()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  // const queryClient = useQueryClient()
  // const { mutate: updateTable } = useUpdateTable()
  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const tableDataCopy = { ...originalData }
    console.log('tableDataCopy', tableDataCopy)
    console.log('updatedSettings', updatedSettings)
    // const editedTableData = updateTableData(tableDataCopy, updatedSettings)
    // console.log('updatedSettings', updatedSettings)
    // console.log('editedTableData', editedTableData)

    // queryClient.setQueryData(['table', tableParam], editedTableData)
    // updateTable(editedTableData, {
    //   onSuccess: (response) => {
    //     queryClient.invalidateQueries({ queryKey: ['table', tableParam] }) // For getting fresh data from database to the cache
    //     console.log('Update successful', response)
    //     setIsEditModalOpen(false)
    //   },
    //   onError: (error) => {
    //     queryClient.setQueryData(['table', tableParam], tableData)

    //     console.error('Error updating table', error)
    //   }
    // })
  }

  return (
    <div className={isDisabled ? 'card-disabled' : 'card'}>
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        {!isNotEditable && <Button title="Edit" onClick={handleOpenModal} />}
      </div>
      <dl className="card-dl">
        {Array.isArray(settings) &&
          settings.map((setting, index) => (
            <Setting key={index} {...setting} />
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
