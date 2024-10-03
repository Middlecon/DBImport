import './Button.scss'
import React from 'react'

interface ButtonProps {
  title: string
  onClick?: () => void
  type?: 'button' | 'reset' | 'submit' | undefined
  lightStyle?: boolean
  fontFamily?: string
  fontSize?: string
}

function Button({
  title,
  type = 'button',
  onClick,
  lightStyle = false,
  fontFamily,
  fontSize
}: ButtonProps) {
  const style: React.CSSProperties = {}
  if (fontFamily) {
    style.fontFamily = fontFamily
  }
  if (fontSize) {
    style.fontSize = fontSize
  }

  return (
    <button
      type={type}
      className={lightStyle ? 'light-button' : 'dark-button'}
      onClick={onClick}
      style={style}
    >
      {title}
    </button>
  )
}

export default Button
