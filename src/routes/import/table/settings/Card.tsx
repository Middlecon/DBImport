import { useState } from 'react'
import './Card.scss'
import Button from '../../../../components/Button'
import EditTableModal from '../../../../components/EditTableModal'
import Setting from './Setting'
import {
  TableSetting,
  TableUpdate,
  UITable
} from '../../../../utils/interfaces'
import { useUpdateTable } from '../../../../utils/mutations'
import {
  getKeyFromLabel,
  reverseMapEnumValue
} from '../../../../utils/nameMappings'
import { useQueryClient } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'

interface CardProps {
  title: string
  settings: TableSetting[]
  tableData: UITable
  isNotEditable?: boolean
  isDisabled?: boolean
}

const fieldsToRemove = [
  'sourceRowcount',
  'sourceRowcountIncr',
  'targetRowcount',
  'validationCustomQueryHiveValue',
  'validationCustomQuerySourceValue',
  'incrMinvalue',
  'incrMaxvalue',
  'incrMinvaluePending',
  'incrMaxvaluePending',
  'lastSize',
  'lastRows',
  'lastMappers',
  'lastExecution',
  'generatedHiveColumnDefinition',
  'generatedSqoopQuery',
  'generatedSqoopOptions',
  'generatedPkColumns',
  'generatedForeignKeys',
  'copyFinished',
  'copySlave'
]

function updateTableData(
  tableData: UITable,
  newSettings: TableSetting[]
): TableUpdate {
  const updatedTableData = { ...tableData }

  newSettings.forEach((setting) => {
    const key = getKeyFromLabel(setting.label)
    if (key) {
      let value = setting.value

      // Handle enum values; assuming enums have a specific type field and possibly enumOptions
      if (setting.type === 'enum' && setting.enumOptions) {
        // This assumes enumOptions maps display names to enum keys
        value = reverseMapEnumValue(setting.label, value as string)
      }

      updatedTableData[key] = value
    }
  })

  const finalTableData = Object.keys(updatedTableData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedTableData[key]
    }
    return acc
  }, {} as TableUpdate)

  return finalTableData
}

function Card({
  title,
  settings,
  tableData,
  isNotEditable,
  isDisabled
}: CardProps) {
  const { table } = useParams<{ table: string }>()
  const queryClient = useQueryClient()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const { mutate: updateTable } = useUpdateTable()
  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (newSettings: TableSetting[]) => {
    const editedTableData = updateTableData(tableData, newSettings)
    // console.log('CARD newSettings', newSettings)
    // console.log('updatedTableData', updateTableData(tableData, newSettings))
    // console.log('editedTableData', editedTableData)

    updateTable(editedTableData, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({ queryKey: ['table', table] })
        console.log('Update successful', response)
        setIsEditModalOpen(false)
      },
      onError: (error) => {
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
