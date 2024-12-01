import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo } from 'react'
import '../_shared/TableDetailedView.scss'

function AirflowDetailedView({
  type
}: {
  type: 'import' | 'export' | 'custom'
}) {
  const { dagName } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  const encodedType = encodeURIComponent(type)
  const encodedDagName = encodeURIComponent(dagName ? dagName : '')

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
      navigate(`/airflow/${encodedType}/${encodedDagName}/settings`, {
        replace: true
      })
    }
  }, [tab, navigate, validTabs, dagName, type, encodedType, encodedDagName])

  const handleTabClick = (tabName: string) => {
    navigate(`/airflow/${encodedType}/${encodedDagName}/${tabName}`)
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
