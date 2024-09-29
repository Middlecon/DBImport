import { useMemo, useState } from 'react'
import './EditTableModal.scss'
import { TableSetting } from '../utils/interfaces'
import Dropdown from './Dropdown'
import { useConnections } from '../utils/queries'
import Button from './Button'

interface EditModalProps {
  title: string
  settings: TableSetting[]
  onSave: (newSettings: TableSetting[]) => void
  onClose: () => void
}

function EditTableModal({ title, settings, onSave, onClose }: EditModalProps) {
  const filteredSettings = settings.filter((setting) => {
    const isReadonly = setting.type === 'readonly'
    const isHidden = setting.isHidden
    const isDatabaseOrTable =
      setting.label === 'Database' || setting.label === 'Table'

    return !isHidden && (!isReadonly || isDatabaseOrTable)
  })
  const { data: connectionsData } = useConnections()
  const connectionNames = useMemo(
    () => connectionsData?.map((connection) => connection.name) ?? [],
    [connectionsData]
  )

  const [editedSettings, setEditedSettings] = useState(filteredSettings)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  console.log('editedSettings', editedSettings)

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean
  ) => {
    const newSettings = [...editedSettings]
    newSettings[index].value = newValue
    setEditedSettings(newSettings)
  }

  const handleSelect = (item: string | number | boolean, keyLabel?: string) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleSave = () => {
    // Merges edited settings back into the original settings array
    const updatedSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )
      return editedSetting ? editedSetting : setting
    })

    onSave(updatedSettings)
    onClose()
  }
  const [showConfirmation, setShowConfirmation] = useState(false)

  const renderEditSetting = (setting: TableSetting, index: number) => {
    const dropdownId = `dropdown-${index}`

    let dropdownOptions: string[] = []
    if (setting.type === 'enum' && setting.enumOptions) {
      dropdownOptions = Object.values(setting.enumOptions)
    }
    switch (setting.type) {
      case 'boolean':
        return (
          <>
            <label>{setting.label}</label>
            <p>{setting.value}</p>
          </>
        )
      case 'readonly':
        return (
          <>
            <label>{setting.label}:</label>
            <span>{setting.value}</span>
          </>
        )

      case 'text':
        return (
          <>
            <label>{setting.label}</label>
            <input
              className="text-input"
              type="text"
              value={setting.value ? String(setting.value) : ''}
              onChange={(e) => handleInputChange(index, e.target.value)}
            />
          </>
        )

      case 'enum':
        return (
          <>
            <label>{setting.label}</label>
            <Dropdown
              keyLabel={setting.label}
              items={dropdownOptions}
              onSelect={handleSelect}
              isOpen={openDropdown === dropdownId}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle(dropdownId, isOpen)
              }
              searchFilter={false}
              initialTitle={String(setting.value)}
              backgroundColor="inherit"
              textColor="black"
              border="0.5px solid rgb(42, 42, 42)"
              borderRadius="3px"
              height="21.5px"
              padding="8px 3px"
              chevronWidth="11"
              chevronHeight="7"
              lightStyle={true}
            />
          </>
        )

      case 'integer':
        return (
          <>
            <label>{setting.label}</label>
            <div>{setting.value}</div>
          </>
        )
      case 'reference':
        return (
          <>
            <label>{setting.label}</label>

            <Dropdown
              keyLabel={setting.label}
              items={
                connectionNames.length > 0 ? connectionNames : ['Loading...']
              }
              onSelect={handleSelect}
              isOpen={openDropdown === dropdownId}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle(dropdownId, isOpen)
              }
              searchFilter={true}
              initialTitle={String(setting.value)}
              backgroundColor="inherit"
              textColor="black"
              border="0.5px solid rgb(42, 42, 42)"
              borderRadius="3px"
              height="21.5px"
              padding="8px 3px"
              chevronWidth="11"
              chevronHeight="7"
              lightStyle={true}
            />
          </>
        )
      case 'hidden':
        return null
      case 'booleanOrAuto(-1)':
        if (typeof setting.value === 'boolean') {
          return (
            <>
              <label>{setting.label}</label>
              <div>{setting.value}</div>
            </>
          )
        } else {
          return (
            <>
              <label>{setting.label}</label>
              <div>Auto</div>
            </>
          )
        }
      case 'integerOrAuto(-1)':
        if (typeof setting.value === 'number' && setting.value > -1) {
          return (
            <>
              <label>{setting.label}</label>
              <div>{setting.value}</div>
            </>
          )
        } else {
          return (
            <>
              <label>{setting.label}</label>
              <div>Auto</div>
            </>
          )
        }
      case 'booleanOrDefaultFromConfig(-1)':
        if (typeof setting.value === 'boolean') {
          return <span>{setting.value ? 'True' : 'False'}</span>
        } else {
          return <span>Default from config</span>
        }

      default:
        return null
    }
  }

  return (
    <div className="modal-backdrop">
      <div className="modal-content">
        <h2>{title}</h2>
        <div className="modal-body">
          {editedSettings.map((setting, index) => (
            <div key={index} className="modal-setting">
              {renderEditSetting(setting, index)}
            </div>
          ))}
        </div>
        <div className="modal-footer">
          <Button title="Cancel" onClick={handleSave} lightStyle={true} />

          <Button title="Save" onClick={handleSave} />
        </div>
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

export default EditTableModal
