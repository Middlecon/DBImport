import { CSSProperties, useRef } from 'react'
import Button from './Button'
import './Modals.scss'
import { useFocusTrap } from '../utils/hooks'
import { isMainSidebarMinimized } from '../atoms/atoms'
import { useAtom } from 'jotai'
import PulseLoader from 'react-spinners/PulseLoader'

interface ConfirmModalProps {
  title: string
  message: string
  buttonTitleCancel: string
  onCancel: () => void
  isActive: boolean
  isLoading?: boolean
  errorInfo?: string | null
  buttonTitleConfirm?: string
  onConfirm?: () => void
}

function ConfirmationModal({
  title,
  message,
  buttonTitleCancel,
  onCancel,
  isActive,
  isLoading,
  errorInfo,
  buttonTitleConfirm,
  onConfirm
}: ConfirmModalProps) {
  const confirmationModalRef = useRef<HTMLDivElement>(null)
  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  useFocusTrap(confirmationModalRef, isActive)

  const override: CSSProperties = {
    display: 'inline',
    marginTop: 5
  }

  return (
    <div className="confirmation-modal-backdrop">
      <div
        className={`confirmation-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        ref={confirmationModalRef}
      >
        <div style={{ display: 'flex' }}>
          <h3 className="confirmation-modal-h3">{title}</h3>
          {isLoading && (
            <PulseLoader loading={isLoading} cssOverride={override} size={3} />
          )}
        </div>

        <p>{!isLoading ? message : ''}</p>

        {!isLoading && errorInfo && (
          <>
            <p>Error message:</p>
            <p className="error-info-message">{errorInfo}</p>
          </>
        )}

        <div className="confirmation-modal-footer">
          <Button
            title={buttonTitleCancel}
            onClick={onCancel}
            lightStyle={true}
          />
          {onConfirm && buttonTitleConfirm && (
            <Button title={buttonTitleConfirm} onClick={onConfirm} />
          )}
        </div>
      </div>
    </div>
  )
}

export default ConfirmationModal
