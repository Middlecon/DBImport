import ChevronDoubleLeft from '../assets/icons/ChevronDoubleLeft'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'
import ChevronDoubleRight from '../assets/icons/ChevronDoubleRight'
import { NavLink, useNavigate } from 'react-router-dom'
import ApacheAirflowIcon from '../assets/icons/ApacheAirflowIcon'
import ConnectionIcon from '../assets/icons/ConnectionIcon'
import ConfigurationIcon from '../assets/icons/ConfigurationIcon'
// import DBImportIconTextLogo from '../assets/icons/DBImportIconTextLogo' // For using the svgs instead of programmed text logo
// import DBImportIconLogo from '../assets/icons/DBImportIconLogo' // For using the svgs instead of programmed text logo
import './MainMenuSidebar.scss'
import LogoWithText from './LogoWithText'
import { useAtom } from 'jotai'
import {
  isAirflowSubmenuActiveAtom,
  selectedExportConnectionAtom,
  selectedImportDatabaseAtom,
  usernameAtom
} from '../atoms/atoms'
import { useState } from 'react'
import AirflowImportIcon from '../assets/icons/AirflowImportIcon'
import AirflowExportIcon from '../assets/icons/AirflowExportIcon'
import AirflowCustomIcon from '../assets/icons/AirflowCustomIcon'
import UserIcon from '../assets/icons/UserIcon'
import LogoutIcon from '../assets/icons/LogoutIcon'
import { useGetStatusAndVersion } from '../utils/queries'
import { deleteCookie } from '../utils/cookies'
import { clearSessionStorageAtoms } from '../atoms/utils'

interface MainSidebarProps {
  minimized: boolean
  setMinimized: React.Dispatch<React.SetStateAction<boolean>>
}
function MainMenuSidebar({ minimized, setMinimized }: MainSidebarProps) {
  const { data: statusData } = useGetStatusAndVersion()
  const navigate = useNavigate()
  const [selectedDatabase] = useAtom(selectedImportDatabaseAtom)
  const [selectedExportConnection] = useAtom(selectedExportConnectionAtom)

  const [toggleAirflowActive, setToggleAirflowActive] = useState(false)
  const [toggleUsernameMenu, setToggleUsernameMenu] = useState(false)
  const [isAirflowSubmenuActive, setIsAirflowSubmenuActive] = useAtom(
    isAirflowSubmenuActiveAtom
  )
  const [userName] = useAtom(usernameAtom)

  const handleToggleAirflowMenu = () => {
    setToggleAirflowActive((prev) => !prev)
  }

  const handleToggleMinimize = () => {
    setMinimized((prevMinimized) => !prevMinimized)
  }

  const handleToggleUsenameMenu = () => {
    setToggleUsernameMenu((prev) => !prev)
  }

  const handleLogout = () => {
    deleteCookie('DBI_auth_token')
    navigate('/login')
    clearSessionStorageAtoms()
  }
  return (
    <>
      <div className={`mainsidebar-root ${minimized ? 'minimized' : ''}`}>
        <div className="mainsidebar-logo-container">
          <NavLink
            to="/"
            className="mainsidebar-logo-link"
            onClick={() => {
              setIsAirflowSubmenuActive(false)
            }}
          >
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
                onClick={() => {
                  setIsAirflowSubmenuActive(false)
                }}
              >
                <ImportIcon />
                {!minimized && <h2>Import</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-menu-li">
              <NavLink
                to={
                  selectedExportConnection
                    ? `/export/${selectedExportConnection}`
                    : '/export'
                }
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ExportIcon />
                {!minimized && <h2>Export</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-menu-li">
              <div
                className={`mainsidebar-menu-link ${
                  isAirflowSubmenuActive ? 'active' : ''
                }`}
                onClick={handleToggleAirflowMenu}
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
                onClick={() => {
                  setIsAirflowSubmenuActive(false)
                }}
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
            <li className="mainsidebar-usermenu-li">
              {toggleUsernameMenu && (
                <ul>
                  <li
                    className="mainsidebar-usermenu mainsidebar-usermenu-logout"
                    onClick={handleLogout}
                  >
                    <LogoutIcon />
                    {!minimized && <h3>Logout</h3>}
                  </li>
                </ul>
              )}
              <div
                className="mainsidebar-usermenu mainsidebar-usermenu-username"
                onClick={handleToggleUsenameMenu}
              >
                <UserIcon />
                {!minimized && <h2>{userName ? userName : null}</h2>}
              </div>
            </li>
          </ul>
        </div>

        <div
          className="chevron-double"
          style={minimized ? { marginBottom: '26px' } : {}}
        >
          <button onClick={handleToggleMinimize}>
            {minimized ? <ChevronDoubleRight /> : <ChevronDoubleLeft />}
          </button>
        </div>
        {!minimized && statusData && (
          <p className="dbimport-version">V{statusData.version}</p>
        )}
      </div>
    </>
  )
}

export default MainMenuSidebar
