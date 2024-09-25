import './Button.scss'

interface EditButtonProps {
  title: string
  onClick: () => void
  lightStyle?: boolean
}

function EditButton({ title, onClick, lightStyle = false }: EditButtonProps) {
  return (
    <button
      className={lightStyle ? 'light-button' : 'dark-button'}
      onClick={onClick}
    >
      {title}
    </button>
  )
}

export default EditButton
