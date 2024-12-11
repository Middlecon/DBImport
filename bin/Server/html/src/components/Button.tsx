import React, { forwardRef, useImperativeHandle, useRef } from 'react'
import './Button.scss'

interface ButtonProps {
  title: string
  onClick?: () => void
  type?: 'button' | 'reset' | 'submit'
  disabled?: boolean
  lightStyle?: boolean
  fontFamily?: string
  fontSize?: string
  padding?: string
  marginRight?: string
}

function Button(props: ButtonProps, ref: React.Ref<HTMLButtonElement>) {
  const {
    title,
    type = 'button',
    disabled,
    onClick,
    lightStyle = false,
    // fontFamily,
    fontSize,
    padding,
    marginRight
  } = props

  const buttonRef = useRef<HTMLButtonElement>(null)

  useImperativeHandle(ref, () => buttonRef.current!)

  const buttonStyle: React.CSSProperties = {
    // fontFamily,
    fontSize,
    padding,
    marginRight
  }

  return (
    <button
      ref={buttonRef}
      type={type}
      className={lightStyle ? 'light-button' : 'dark-button'}
      onClick={onClick}
      style={buttonStyle}
      disabled={disabled}
    >
      <div className="button-title">{title}</div>
    </button>
  )
}

export default forwardRef(Button)
