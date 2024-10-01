import ChevronDoubleLeft from '../assets/icons/ChevronDoubleLeft'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'
import LogoWithText from './LogoWithText'

import './MainMenuSidebar.scss'
import ChevronDoubleRight from '../assets/icons/ChevronDoubleRight'
import { NavLink } from 'react-router-dom'
import ApacheAirflowIcon from '../assets/icons/ApacheAirflowIcon'
import ConnectionIcon from '../assets/icons/ConnectionIcon'
import ConfigurationIcon from '../assets/icons/ConfigurationIcon'

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
          <LogoWithText
            fontSize="10px"
            logoSize="40px"
            textMarginTop="4.5%"
            textMarginLeft="5px"
            noText={minimized ? true : false}
          />
        </div>
        <div>
          <ul>
            <li>
              <NavLink
                to="/import"
                className={({ isActive }) =>
                  `menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ImportIcon />
                {!minimized && <h2>Import</h2>}
              </NavLink>
            </li>
            <li>
              <NavLink
                to="/export"
                className={({ isActive }) =>
                  `menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ExportIcon />
                {!minimized && <h2>Export</h2>}
              </NavLink>
            </li>
            <li>
              <NavLink
                to="/airflow"
                className={({ isActive }) =>
                  `menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ApacheAirflowIcon />
                {!minimized && <h2>Airflow</h2>}
              </NavLink>
            </li>
            <li>
              <NavLink
                to="/connection"
                className={({ isActive }) =>
                  `menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ConnectionIcon />
                {!minimized && <h2>Connection</h2>}
              </NavLink>
            </li>
            <li>
              <NavLink
                to="/configuration"
                className={({ isActive }) =>
                  `menu-link ${isActive ? 'active' : ''}`
                }
              >
                <ConfigurationIcon />
                {!minimized && <h2>Configuration</h2>}
              </NavLink>
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
