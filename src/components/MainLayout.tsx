import React, { useState } from 'react'
import { Outlet } from 'react-router-dom'
import MainMenuSidebar from './MainMenuSidebar'
import './MainLayout.scss'

const MainLayout: React.FC = () => {
  const [minimized, setMinimized] = useState(false)

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <MainMenuSidebar minimized={minimized} setMinimized={setMinimized} />
      <div className={`outlet-container ${minimized ? 'minimized' : ''}`}>
        <Outlet />
      </div>
    </div>
  )
}

export default MainLayout
