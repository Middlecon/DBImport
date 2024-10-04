import './Button.scss'
import React from 'react'

interface ButtonProps {
  title: string
  onClick?: () => void
  type?: 'button' | 'reset' | 'submit' | undefined
  lightStyle?: boolean
  fontFamily?: string
  fontSize?: string
  padding?: string
}

function Button({
  title,
  type = 'button',
  onClick,
  lightStyle = false,
  fontFamily,
  fontSize,
  padding
}: ButtonProps) {
  const style: React.CSSProperties = {}
  if (fontFamily) {
    style.fontFamily = fontFamily
  }
  if (fontSize) {
    style.fontSize = fontSize
  }

  const buttonStyle: React.CSSProperties = {
    fontFamily: fontFamily,
    fontSize: fontSize,
    padding: padding
  }

  return (
    <button
      type={type}
      className={lightStyle ? 'light-button' : 'dark-button'}
      onClick={onClick}
      style={buttonStyle}
    >
      <div className="button-title">{title}</div>
    </button>
  )
}

export default Button
