import { useRef } from 'react'
import Button from './Button'
import './ConfirmationModal.scss'
import { useFocusTrap } from '../utils/hooks'

interface ConfirmModalProps {
  title: string
  message: string
  buttonTitleCancel: string
  buttonTitleConfirm: string
  onConfirm: () => void
  onCancel: () => void
  isActive: boolean
}

function ConfirmationModal({
  title,
  message,
  buttonTitleCancel,
  buttonTitleConfirm,
  onConfirm,
  onCancel,
  isActive
}: ConfirmModalProps) {
  const confirmationModalRef = useRef<HTMLDivElement>(null)

  useFocusTrap(confirmationModalRef, isActive)

  return (
    <div className="confirmation-modal-backdrop">
      <div className="confirmation-modal-content" ref={confirmationModalRef}>
        <h3 className="confirmation-modal-h3">{title}</h3>
        <p>{message}</p>
        <div className="confirmation-modal-footer">
          <Button
            title={buttonTitleCancel}
            onClick={onCancel}
            lightStyle={true}
          />
          <Button title={buttonTitleConfirm} onClick={onConfirm} />
        </div>
      </div>
    </div>
  )
}

export default ConfirmationModal
