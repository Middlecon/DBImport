import { CSSProperties } from 'react'
import './LogoWithText.scss'

interface LogoWithTextProps {
  fontSize: string
  logoSize: string
  textMarginTop?: string | undefined
  textMarginLeft?: string | undefined
  noText?: boolean
}

function LogoWithText({
  fontSize,
  logoSize,
  textMarginTop,
  textMarginLeft,
  noText = false
}: LogoWithTextProps) {
  const style: CSSProperties & { [key: string]: string } = {
    '--font-size': fontSize,
    '--logo-size': logoSize,
    '--text-margin-top': textMarginTop ? textMarginTop : '',
    '--text-margin-left': textMarginLeft ? textMarginLeft : ''
  }

  return (
    <div className="logo-w-name-root">
      <div className="logo-container" style={style}>
        <img className="logo" src="/dbimport_logo.webp" alt="dbimport_logo" />
        {!noText && <h1>DBImport</h1>}
      </div>
    </div>
  )
}

export default LogoWithText
