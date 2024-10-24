import Button from './Button'
import './ConfirmationModal.scss'

interface ConfirmCancelModalProps {
  onConfirm: () => void
  onCancel: () => void
  message: string
}

function ConfirmationModal({
  onConfirm,
  onCancel,
  message
}: ConfirmCancelModalProps) {
  return (
    <div className="confirmation-modal-backdrop">
      <div className="confirmation-modal-content">
        <h3 className="confirmation-modal-h3">Cancel Edit</h3>
        <p>{message}</p>
        <div className="confirmation-modal-footer">
          <Button title="No, Go Back" onClick={onCancel} lightStyle={true} />
          <Button title="Yes, Cancel" onClick={onConfirm} />
        </div>
      </div>
    </div>
  )
}

export default ConfirmationModal
