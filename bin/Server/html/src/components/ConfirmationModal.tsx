import { useRef } from 'react'
import Button from './Button'
import './ConfirmationModal.scss'
import { useFocusTrap } from '../utils/hooks'
import { isMainSidebarMinimized } from '../atoms/atoms'
import { useAtom } from 'jotai'

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
  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  useFocusTrap(confirmationModalRef, isActive)

  return (
    <div className="confirmation-modal-backdrop">
      <div
        className={`confirmation-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        ref={confirmationModalRef}
      >
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
