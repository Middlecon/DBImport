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
import { useAtom } from 'jotai'
import { selectedImportDatabaseAtom } from '../atoms/atoms'
import { useState } from 'react'
import AirflowImportIcon from '../assets/icons/AirflowImportIcon'
import AirflowExportIcon from '../assets/icons/AirflowExportIcon'
import AirflowCustomIcon from '../assets/icons/AirflowCustomIcon'

interface MainSidebarProps {
  minimized: boolean
  setMinimized: React.Dispatch<React.SetStateAction<boolean>>
}
function MainMenuSidebar({ minimized, setMinimized }: MainSidebarProps) {
  const [selectedDatabase] = useAtom(selectedImportDatabaseAtom)
  const [isAirflowActive, setIsAirflowActive] = useState(false)
  const [toggleAirflowActive, setToggleAirflowActive] = useState(false)

  const [isAirflowSubmenuActive, setIsAirflowSubmenuActive] = useState(false)

  const handleAirflowClick = () => {
    setToggleAirflowActive((prev) => !prev)
  }

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
            <li className="mainsidebar-menu-li">
              <NavLink
                to={
                  selectedDatabase ? `/import/${selectedDatabase}` : '/import'
                }
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
                onClick={() => setIsAirflowActive(false)}
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
            <li className="mainsidebar-menu-li">
              <div
                className={`mainsidebar-menu-link ${
                  isAirflowActive && isAirflowSubmenuActive ? 'active' : ''
                }`}
                onClick={handleAirflowClick}
              >
                <ApacheAirflowIcon />
                {!minimized && <h2>Airflow</h2>}
              </div>
              {toggleAirflowActive && (
                <ul>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/import"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowActive(true)
                        setIsAirflowSubmenuActive(true)
                      }}
                    >
                      <AirflowImportIcon />
                      {!minimized && <h3>Import</h3>}
                    </NavLink>
                  </li>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/export"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowActive(true)
                        setIsAirflowSubmenuActive(true)
                      }}
                    >
                      <AirflowExportIcon />

                      {!minimized && <h3>Export</h3>}
                    </NavLink>
                  </li>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/custom"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowActive(true)
                        setIsAirflowSubmenuActive(true)
                      }}
                    >
                      <AirflowCustomIcon />

                      {!minimized && <h3>Custom</h3>}
                    </NavLink>
                  </li>
                </ul>
              )}
            </li>
            <li className="mainsidebar-menu-li">
              <NavLink
                to="/connection"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
                onClick={() => setIsAirflowActive(false)}
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
