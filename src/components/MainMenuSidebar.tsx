import ChevronDoubleLeft from '../assets/icons/ChevronDoubleLeft'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'
import ChevronDoubleRight from '../assets/icons/ChevronDoubleRight'
import { NavLink } from 'react-router-dom'
import ApacheAirflowIcon from '../assets/icons/ApacheAirflowIcon'
import ConnectionIcon from '../assets/icons/ConnectionIcon'
import ConfigurationIcon from '../assets/icons/ConfigurationIcon'
// import DBImportIconTextLogo from '../assets/icons/DBImportIconTextLogo' // For using the svgs instead of programmed text logo
// import DBImportIconLogo from '../assets/icons/DBImportIconLogo' // For using the svgs instead of programmed text logo
import './MainMenuSidebar.scss'
import LogoWithText from './LogoWithText'

interface MainSidebarProps {
  minimized: boolean
  setMinimized: React.Dispatch<React.SetStateAction<boolean>>
}
function MainMenuSidebar({ minimized, setMinimized }: MainSidebarProps) {
  const handleToggleMinimize = () => {
    setMinimized((prevMinimized) => !prevMinimized)
  }

  return (
    <>
      <div className={`mainsidebar-root ${minimized ? 'minimized' : ''}`}>
        <div className="mainsidebar-logo-container">
          <NavLink to="/" className="mainsidebar-logo-link">
            <LogoWithText size="small" noText={minimized ? true : false} />
            {/* {minimized ? (
            <DBImportIconLogo />
          ) : (
            <>
              <DBImportIconTextLogo size="small" />
            </>
          )} */}
          </NavLink>
        </div>
        {/* <p style={{ fontSize: '5.5px', color: ' white', marginLeft: 88 }}> // To compare sharpness to logo svg text
          Powered by
        </p> */}

        <div className="menu-options">
          <ul>
            <li>
              <NavLink
                to="/import"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ImportIcon />
                {!minimized && <h2>Import</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-disabled">
              {/* <NavLink
                to="/export"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              > */}
              <ExportIcon />
              {!minimized && <h2>Export</h2>}
              {/* </NavLink> */}
            </li>
            <li className="mainsidebar-disabled">
              {/* <NavLink
                to="/airflow"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              > */}
              <ApacheAirflowIcon />
              {!minimized && <h2>Airflow</h2>}
              {/* </NavLink> */}
            </li>
            <li>
              <NavLink
                to="/connection"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ConnectionIcon />
                {!minimized && <h2>Connection</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-disabled">
              {/* <NavLink
                to="/configuration"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              > */}
              <ConfigurationIcon />
              {!minimized && <h2>Configuration</h2>}
              {/* </NavLink> */}
            </li>
          </ul>
        </div>

        <div className="chevron-double">
          <button onClick={handleToggleMinimize}>
            {minimized ? <ChevronDoubleRight /> : <ChevronDoubleLeft />}
          </button>
        </div>
      </div>
    </>
  )
}

export default MainMenuSidebar
