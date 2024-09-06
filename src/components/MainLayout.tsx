import React from 'react'
import { Outlet } from 'react-router-dom'
import MainSidebar from './MainSidebar'

const MainLayout: React.FC = () => {
  return (
    <div style={{ display: 'flex' }}>
      <MainSidebar />
      <div style={{ flex: 1, padding: '20px' }}>
        <Outlet />
      </div>
    </div>
  )
}

export default MainLayout
