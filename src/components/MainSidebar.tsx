import LogoWithText from './LogoWithText'
import './MainSidebar.scss'

function MainSidebar() {
  return (
    <>
      <div className="mainsidebar-root">
        <div className="mainsidebar-logo-container">
          <LogoWithText fontSize="10px" logoSize="40px" textMarginTop="4.5%" />
        </div>
      </div>
    </>
  )
}

export default MainSidebar
