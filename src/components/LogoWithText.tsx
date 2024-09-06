import { CSSProperties } from 'react'
import './LogoWithText.scss'

function LogoWithText({
  fontSize,
  logoSize,
  textMarginTop
}: {
  fontSize: string
  logoSize: string
  textMarginTop: string
}) {
  const style: CSSProperties & { [key: string]: string } = {
    '--font-size': fontSize,
    '--logo-size': logoSize,
    '--text-margin-top': textMarginTop
  }

  return (
    <div className="logo-w-name-root">
      <div className="logo-container" style={style}>
        <img
          className="logo"
          src="../public/dbimport_logo.webp"
          alt="dbimport_logo"
        />
        <h1>DBImport</h1>
      </div>
    </div>
  )
}

export default LogoWithText
