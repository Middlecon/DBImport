import { useMemo, useState } from 'react'
import {
  EtlEngine,
  EtlType,
  ImportTool,
  ImportType,
  // EtlEngine,
  // EtlType,
  // ImportTool,
  // ImportType,
  SettingType
} from '../utils/enums'
import { TableSetting, TableSettingsValueTypes } from '../utils/interfaces'
import { useConnections } from '../utils/queries'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../utils/TableInputFields'
import { getEnumOptions, mapDisplayValue } from '../utils/nameMappings'
import RequiredFieldsInfo from './RequiredFieldsInfo'
import './Modals.scss'

interface CreateTableModalProps {
  database: string
  prefilledConnection: string
  onSave: (newTableData: TableSetting[]) => void
  onClose: () => void
}

function initialCreateTableSeetings(
  database: string,
  prefilledConnection: string
) {
  //   const settings: TableSetting[] = [
  //     { label: 'Database', value: database, type: SettingType.Text }, //Free-text, read-only, default selected db, potentially copyable?
  //     { label: 'Table', value: null, type: SettingType.Text }, // Free-text, read-only
  //     {
  //       label: '',
  //       value: '',
  //       type: SettingType.GroupingSpace
  //     }, // Layout space
  //     {
  //       label: 'Connection',
  //       value: prefilledConnection,
  //       type: SettingType.ConnectionReference
  //     }, // Reference to /connection
  //     {
  //       label: 'Source Schema',
  //       value: null,
  //       type: SettingType.Text
  //     }, // Free-text setting
  //     { label: 'Source Table', value: '', type: SettingType.Text }, // Free-text setting
  //     {
  //       label: '',
  //       value: '',
  //       type: SettingType.GroupingSpace
  //     }, // Layout space
  //     {
  //       label: 'Import Type',
  //       value: 'Full',
  //       type: SettingType.Enum,
  //       enumOptions: getEnumOptions('importPhaseType')
  //     }, // Enum mapping for 'Import Type'
  //     {
  //       label: 'ETL Type',
  //       value: 'Truncate and Insert',
  //       type: SettingType.Enum,
  //       enumOptions: getEnumOptions('etlPhaseType')
  //     }, // Enum mapping for 'ETL Type'
  //     {
  //       label: 'Import Tool',
  //       value: 'Spark',
  //       type: SettingType.Enum,
  //       enumOptions: getEnumOptions('importTool')
  //     }, // Enum mapping for 'Import Tool'
  //     {
  //       label: 'ETL Engine',
  //       value: 'Spark',
  //       type: SettingType.Enum,
  //       enumOptions: getEnumOptions('etlEngine')
  //     } // Enum mapping for 'ETL Engine'
  //   ]
  //   return settings
  // }

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
      value: prefilledConnection,
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
      value: mapDisplayValue('importPhaseType', ImportType.Full),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importPhaseType')
    }, // Enum mapping for 'Import Type'
    {
      label: 'ETL Type',
      value: mapDisplayValue('etlPhaseType', EtlType.TruncateAndInsert),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlPhaseType')
    }, // Enum mapping for 'ETL Type'
    {
      label: 'Import Tool',
      value: mapDisplayValue('importTool', ImportTool.Spark),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importTool')
    }, // Enum mapping for 'Import Tool'
    {
      label: 'ETL Engine',
      value: mapDisplayValue('etlEngine', EtlEngine.Spark),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlEngine')
    } // Enum mapping for 'ETL Engine'
  ]
  return settings
}

function CreateTableModal({
  database,
  prefilledConnection,
  onSave,
  onClose
}: CreateTableModalProps) {
  const settings = initialCreateTableSeetings(database, prefilledConnection)
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const [editedSettings, setEditedSettings] = useState<TableSetting[]>(settings)
  const [showConfirmation, setShowConfirmation] = useState(false)

  const isRequiredFieldEmpty = useMemo(() => {
    const requiredLabels = [
      'Database',
      'Table',
      'Source Schema',
      'Source Table'
    ]
    return editedSettings.some(
      (setting) => requiredLabels.includes(setting.label) && !setting.value
    )
  }, [editedSettings])

  const handleInputChange = (
    index: number,
    newValue: TableSettingsValueTypes | null
  ) => {
    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

    setEditedSettings(updatedSettings)
  }

  const handleSelect = (
    item: TableSettingsValueTypes | null,
    keyLabel?: string
  ) => {
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
    <div className="table-modal-backdrop">
      <div className="table-modal-content">
        <h2 className="table-modal-h2">Create table</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            handleSave()
          }}
        >
          <div className="table-modal-body">
            {editedSettings &&
              editedSettings.map((setting, index) => (
                <div key={index} className="table-modal-setting">
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    connectionNames={connectionNames}
                  />
                </div>
              ))}
          </div>
          <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />

          <div className="table-modal-footer">
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
  )
}

export default CreateTableModal
