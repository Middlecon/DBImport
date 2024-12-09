import { useCallback, useEffect, useState } from 'react'
import { BulkField } from '../utils/interfaces'
import Button from './Button'
import ConfirmationModal from './ConfirmationModal'
import './Modals.scss'
import BulkInputFields, { BulkInputFieldsProps } from '../utils/BulkInputFields'

interface BulkEditModalProps<T> {
  title: string
  selectedRows: T[]
  bulkFieldsData: BulkField<T>[]
  onSave: (
    bulkChanges: Partial<Record<keyof T, string | number | boolean | null>>
  ) => void
  onClose: () => void
  initWidth?: number
}

function BulkEditModal<T>({
  title,
  selectedRows,
  bulkFieldsData,
  onSave,
  onClose,
  initWidth
}: BulkEditModalProps<T>) {
  const [bulkChanges, setBulkChanges] = useState<
    Partial<Record<keyof T, string | number | boolean | null>>
  >({})

  const handleBulkChangeInternal = useCallback(
    (fieldKey: keyof T, newValue: string | number | boolean | null) => {
      setBulkChanges((prev) => ({ ...prev, [fieldKey]: newValue }))
    },
    []
  )

  const [showConfirmation, setShowConfirmation] = useState(false)
  const [modalWidth, setModalWidth] = useState(initWidth ?? 584)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(initWidth ?? 584)
  const MIN_WIDTH = initWidth ?? 584

  const handleSave = () => {
    onSave(bulkChanges)
    onClose()
  }

  const handleCancelClick = () => {
    const hasChanges = Object.keys(bulkChanges).length > 0
    if (hasChanges) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setBulkChanges({})
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsResizing(true)
    setInitialMouseX(e.clientX)
    setInitialWidth(modalWidth)
  }

  const handleMouseUp = useCallback(() => {
    setIsResizing(false)
  }, [])

  const handleMouseMove = useCallback(
    (e: MouseEvent) => {
      if (isResizing) {
        const deltaX = e.clientX - initialMouseX
        setModalWidth(Math.max(initialWidth + deltaX, MIN_WIDTH))
      }
    },
    [isResizing, initialMouseX, initialWidth, MIN_WIDTH]
  )

  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    } else {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing, handleMouseMove, handleMouseUp])

  const BulkInputFieldsTyped = BulkInputFields as React.FC<
    BulkInputFieldsProps<T>
  >

  return (
    <div className="table-modal-backdrop">
      <div className="table-modal-content" style={{ width: `${modalWidth}px` }}>
        <div
          className="table-modal-resize-handle left"
          onMouseDown={handleMouseDown}
        ></div>
        <div
          className="table-modal-resize-handle right"
          onMouseDown={handleMouseDown}
        ></div>
        <h2 className="table-modal-h2">{title}</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            handleSave()
          }}
        >
          <div className="table-modal-body">
            <BulkInputFieldsTyped
              fields={bulkFieldsData}
              selectedRows={selectedRows}
              bulkChanges={bulkChanges}
              onBulkChange={handleBulkChangeInternal}
            />
          </div>

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
          title="Cancel Edit"
          message="Any unsaved changes will be lost."
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Cancel"
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          isActive={showConfirmation}
        />
      )}
    </div>
  )
}

export default BulkEditModal
