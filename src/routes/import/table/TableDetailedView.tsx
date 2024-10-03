import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import './TableDetailedView.scss'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import { useEffect, useMemo } from 'react'

function TableDetailedView() {
  const { database, table } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  const pathSegments = location.pathname.split('/').filter(Boolean)
  const tab = pathSegments[3] || 'settings'

  const validTabs = useMemo(() => ['settings', 'columns', 'statistics'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      navigate(`/import/${database}/${table}/settings`, { replace: true })
    }
  }, [tab, database, table, navigate, validTabs])

  const handleTabClick = (tabName: string) => {
    navigate(`/import/${database}/${table}/${tabName}`)
  }

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import', `${database}`, `${table}`]}>
        <div className="table-header">
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
