import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo } from 'react'
import '../_shared/TableDetailedView.scss'

function AirflowDetailedView({ type }: { type: string }) {
  const { dagName } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  // const pathSegments = location.pathname.split('/').filter(Boolean)
  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab = pathSegments[3] || 'settings'
  const validTabs = useMemo(() => ['settings', 'tasks'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      navigate(`/airflow/${type}/${dagName}/settings`, { replace: true })
    }
  }, [tab, navigate, validTabs, dagName, type])

  const handleTabClick = (tabName: string) => {
    navigate(`/airflow/${type}/${dagName}/${tabName}`)
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>{`${dagName}`}</h1>
        </div>
        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'tasks' ? 'active-tab' : ''}
            onClick={() => handleTabClick('tasks')}
          >
            Tasks
          </h2>
        </div>
        <Outlet />
      </ViewBaseLayout>
    </>
  )
}

export default AirflowDetailedView
