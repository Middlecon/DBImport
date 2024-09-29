import { useState } from 'react'
import './Card.scss'
import Button from '../../../../components/Button'
import EditTableModal from '../../../../components/EditTableModal'
import Setting from './Setting'
import { TableSetting } from '../../../../utils/interfaces'
// import { useMutation, useQueryClient } from '@tanstack/react-query'

interface CardProps {
  title: string
  settings: TableSetting[]
  isNotEditable?: boolean
  isDisabled?: boolean
}

function Card({ title, settings, isNotEditable, isDisabled }: CardProps) {
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)

  // const [currentSettings, setCurrentSettings] = useState(settings)
  // console.log('currentSettings', currentSettings)

  //////////////////////////////////////////////////
  // const queryClient = useQueryClient()
  // const mutation = useMutation<Response, Error, TableSetting[]>(
  //   (newSettings: TableSetting[]) => {
  //     // API call to update settings
  //     return fetch('/api/update-settings', {
  //       method: 'POST',
  //       headers: {
  //         'Content-Type': 'application/json'
  //       },
  //       body: JSON.stringify(newSettings)
  //     })
  //   },
  //   {
  //     onSuccess: () => {
  //       queryClient.invalidateQueries(['table'])
  //     }
  //   }
  // )
  //////////////////////////////////////////////////

  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  // const handleSave = (newSettings: typeof currentSettings) => {
  //   setCurrentSettings(newSettings)
  // }
  const handleSave = (newSettings: TableSetting[]) => {
    console.log('CARD newSettings', newSettings)
    // mutation.mutate(newSettings)
    setIsEditModalOpen(false)
  }

  return (
    <div className={isDisabled ? 'card-disabled' : 'card'}>
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        {!isNotEditable && <Button title="Edit" onClick={handleOpenModal} />}
      </div>
      <dl className="card-dl">
        {settings.map((setting, index) => (
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
