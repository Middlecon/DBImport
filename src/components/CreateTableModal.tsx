import { useMemo, useState } from 'react'
import {
  // EtlEngine,
  // EtlType,
  // ImportTool,
  // ImportType,
  SettingType
} from '../utils/enums'
import { TableSetting } from '../utils/interfaces'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import { getEnumOptions } from '../utils/nameMappings'
import './EditTableModal.scss'

interface CreateTableModalProps {
  database: string
  onSave: (newTableData: TableSetting[]) => void
  onClose: () => void
}

// const initialSettings = {
//   database: '',
//   table: '',
//   connection: '',
//   sourceSchema: '',
//   sourceTable: '',
//   importPhaseType: null,
//   etlPhaseType: null,
//   importTool: null,
//   etlEngine: null
// }

// const newSettingsExample = {
//   database: '',
//   table: '',
//   connection: '',
//   sourceSchema: '',
//   sourceTable: '',
//   importPhaseType: ImportType.Full,
//   etlPhaseType: EtlType.None,
//   importTool: ImportTool.Spark,
//   etlEngine: EtlEngine.Spark
// }

function initialCreateTableSeetings(database: string) {
  const settings: TableSetting[] = [
    { label: 'Database', value: database, type: SettingType.Text }, //Free-text, read-only, default selected db, potentially copyable?
    { label: 'Table', value: null, type: SettingType.Text }, // Free-text, read-only
    {
      label: '',
      value: '',
      type: SettingType.GroupingSpace
    }, // Layout space
    {
      label: 'Connection',
      value: 'Select...',
      type: SettingType.ConnectionReference
    }, // Reference to /connection
    {
      label: 'Source Schema',
      value: null,
      type: SettingType.Text
    }, // Free-text setting
    { label: 'Source Table', value: '', type: SettingType.Text }, // Free-text setting
    {
      label: '',
      value: '',
      type: SettingType.GroupingSpace
    }, // Layout space
    {
      label: 'Import Type',
      value: 'Select...',
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importPhaseType')
    }, // Enum mapping for 'Import Type'
    {
      label: 'ETL Type',
      value: 'Select...',
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlPhaseType')
    }, // Enum mapping for 'ETL Type'
    {
      label: 'Import Tool',
      value: 'Select...',
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importTool')
    }, // Enum mapping for 'Import Tool'
    {
      label: 'ETL Engine',
      value: 'Select...',
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlEngine')
    } // Enum mapping for 'ETL Engine'
  ]
  return settings
}

function CreateTableModal({
  database,
  onSave,
  onClose
}: CreateTableModalProps) {
  const settings = initialCreateTableSeetings(database)
  const { data: connectionsData } = useConnections()
  const connectionNames = useMemo(
    () => connectionsData?.map((connection) => connection.name) ?? [],
    [connectionsData]
  )

  const [editedSettings, setEditedSettings] = useState<TableSetting[]>([])
  const [prevValue, setPrevValue] = useState<string | number | boolean>('')
  const [showConfirmation, setShowConfirmation] = useState(false)

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null
  ) => {
    // Creates a new array, copying all elements of editedSettings
    const newSettings = [...editedSettings]

    const currentValue = newSettings[index].value

    // Only stores previous value if it's a whole number
    if (newValue === -1) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== -1 &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }

    // Stores previous value if it's a whole number when newValue is null
    if (newValue === null) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== null &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }

    // Creates a new object for the setting being updated
    const updatedSetting = { ...newSettings[index], value: newValue }

    // Replaces the old object in the array with the new object
    newSettings[index] = updatedSetting
    setEditedSettings(newSettings)
  }

  // const handleInputChange = (
  //   index: number,
  //   newValue: string | number | boolean | null
  // ) => {
  //   const newSettings = [...editedSettings]
  //   const currentValue = newSettings[index].value

  //   // Only stores previous value if it's a whole number
  //   if (newValue === -1) {
  //     if (
  //       typeof currentValue === 'number' &&
  //       currentValue !== -1 &&
  //       Number.isInteger(currentValue)
  //     ) {
  //       setPrevValue(currentValue)
  //     }
  //   }

  //   // Stores previous value if it's a whole number when newValue is null
  //   if (newValue === null) {
  //     if (
  //       typeof currentValue === 'number' &&
  //       currentValue !== null &&
  //       Number.isInteger(currentValue)
  //     ) {
  //       setPrevValue(currentValue)
  //     }
  //   }

  //   // Sets the new value
  //   newSettings[index].value = newValue
  //   setEditedSettings(newSettings)
  // }

  const handleSelect = (item: string | number | boolean, keyLabel?: string) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleSave = () => {
    const newTableSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    onSave(newTableSettings)
    onClose()
  }

  const handleCancelClick = () => {
    setShowConfirmation(true)
  }

  const handleConfirmCancel = () => {
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  return (
    <div className="edit-table-modal-backdrop">
      <div className="edit-table-modal-content">
        <h2 className="edit-table-modal-h2">Create table</h2>
        <form
          onSubmit={(e) => {
            e.preventDefault()
            handleSave()
          }}
        >
          <div className="edit-table-modal-body">
            {settings.map((setting, index) => (
              <div key={index} className="edit-table-modal-setting">
                <TableInputFields
                  index={index}
                  setting={setting}
                  handleInputChange={handleInputChange}
                  handleSelect={handleSelect}
                  prevValue={prevValue}
                  connectionNames={connectionNames}
                />
              </div>
            ))}
          </div>
          <div className="edit-table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button type="submit" title="Save" />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          message="Any unsaved changes will be lost."
        />
      )}
    </div>
    // <div className="confirmation-modal-backdrop">
    //   <div className="confirmation-modal-content">
    //     <h3 className="confirmation-modal-h3">Create table</h3>
    //     <div className="confirmation-modal-footer">
    //       <Button title="Cancel" onClick={onClose} lightStyle={true} />
    //       <Button type="submit" title="Save" />
    //     </div>
    //   </div>
    // </div>
  )
}

export default CreateTableModal
