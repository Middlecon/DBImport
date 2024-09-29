import './Button.scss'

interface ButtonProps {
  title: string
  onClick?: () => void
  type?: 'button' | 'reset' | 'submit' | undefined

  lightStyle?: boolean
}

function Button({
  title,
  type = 'button',
  onClick,
  lightStyle = false
}: ButtonProps) {
  return (
    <button
      type={type}
      className={lightStyle ? 'light-button' : 'dark-button'}
      onClick={onClick}
    >
      {title}
    </button>
  )
}

export default Button
