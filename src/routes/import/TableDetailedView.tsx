import { Outlet, useNavigate, useParams } from 'react-router-dom'
import './TableDetailedView.scss'
import ViewBaseLayout from '../../components/ViewBaseLayout'
// import { useTable } from '../../utils/queries';
import { useState } from 'react'

function TableDetailedView() {
  const { database, table } = useParams()
  // const { data } = useTable();
  const [selectedTab, setSelectedTab] = useState('')
  const navigate = useNavigate()

  const handleTabClick = (tabName: string) => {
    setSelectedTab(tabName)
    navigate(`/import/${database}/${table}/${tabName}`)
  }

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import', `${database}`, `${table}`]}>
        <div className="import-header">
          <h1>{`${database}.${table}`}</h1>
        </div>
        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'columns' ? 'active-tab' : ''}
            onClick={() => handleTabClick('columns')}
          >
            Columns
          </h2>
          <h2
            className={selectedTab === 'statistics' ? 'active-tab' : ''}
            onClick={() => handleTabClick('statistics')}
          >
            Statistics
          </h2>
        </div>
        <Outlet />
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
