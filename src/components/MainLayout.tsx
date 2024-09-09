import React from 'react'
import { Outlet } from 'react-router-dom'
import MainSidebar from './MainMenuSidebar'
import './MainLayout.scss'

const MainLayout: React.FC = () => {
  return (
    <div style={{ display: 'flex' }}>
      <MainSidebar />
      <div className="outlet-container">
        <Outlet />
      </div>
    </div>
  )
}

export default MainLayout
