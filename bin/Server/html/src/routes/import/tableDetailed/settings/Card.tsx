import { useState } from 'react'
import './Card.scss'
import Button from '../../../../components/Button'
import EditTableModal from '../../../../components/EditTableModal'
import Setting from './Setting'
import { EditSetting, UITable } from '../../../../utils/interfaces'
import { useUpdateTable } from '../../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'
import { updateTableData } from '../../../../utils/dataFunctions'

interface CardProps {
  title: string
  settings: EditSetting[]
  tableData: UITable
  isNotEditable?: boolean
  isDisabled?: boolean
}

function Card({
  title,
  settings,
  tableData,
  isNotEditable,
  isDisabled
}: CardProps) {
  const { table: tableParam } = useParams<{ table: string }>()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const tableDataCopy = { ...tableData }
    const editedTableData = updateTableData(tableDataCopy, updatedSettings)
    console.log('updatedSettings', updatedSettings)
    console.log('editedTableData', editedTableData)

    queryClient.setQueryData(['table', tableParam], editedTableData)
    updateTable(editedTableData, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({ queryKey: ['table', tableParam] }) // For getting fresh data from database to the cache
        console.log('Update successful', response)
        setIsEditModalOpen(false)
      },
      onError: (error) => {
        queryClient.setQueryData(['table', tableParam], tableData)

        console.error('Error updating table', error)
      }
    })
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
        <EditTableModal
          title={`Edit ${title}`}
          settings={settings}
          onSave={handleSave}
          onClose={handleCloseModal}
        />
      )}
    </div>
  )
}

export default Card
