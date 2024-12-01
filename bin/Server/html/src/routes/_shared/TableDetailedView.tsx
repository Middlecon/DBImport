import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo } from 'react'
import './TableDetailedView.scss'

interface TableDetailedViewProps {
  type: 'import' | 'export'
}

function TableDetailedView({ type }: TableDetailedViewProps) {
  const { database, table, connection, targetSchema, targetTable } = useParams()
  const encodedDatabase = encodeURIComponent(database ? database : '')
  const encodedTable = encodeURIComponent(table ? table : '')
  const encodedConnection = encodeURIComponent(connection ? connection : '')
  const encodedTargetSchema = encodeURIComponent(
    targetSchema ? targetSchema : ''
  )
  const encodedTargetTable = encodeURIComponent(targetTable ? targetTable : '')

  const navigate = useNavigate()
  const location = useLocation()

  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab =
    type === 'export'
      ? pathSegments[4] || 'settings'
      : pathSegments[3] || 'settings'

  const validTabs = useMemo(() => ['settings', 'columns', 'statistics'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      if (type === 'import') {
        navigate(`/import/${encodedDatabase}/${encodedTable}/settings`, {
          replace: true
        })
      } else if (type === 'export') {
        navigate(
          `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/settings`,
          {
            replace: true
          }
        )
      }
    }
  }, [
    tab,
    database,
    table,
    navigate,
    validTabs,
    type,
    connection,
    encodedDatabase,
    encodedTable,
    encodedConnection,
    encodedTargetSchema,
    encodedTargetTable
  ])

  const handleTabClick = (tabName: string) => {
    if (type === 'import') {
      navigate(`/import/${encodedDatabase}/${encodedTable}/${tabName}`)
    } else if (type === 'export') {
      navigate(
        `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/${tabName}`
      )
    }
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          {type === 'import' && <h1>{`${database}.${table}`}</h1>}
          {type === 'export' && (
            <h1>{`${connection}.${targetSchema}.${targetTable}`}</h1>
          )}
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
            style={{ color: '#a4a4a4', cursor: 'default' }}
            // className={selectedTab === 'statistics' ? 'active-tab' : ''}
            // onClick={() => handleTabClick('statistics')}
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
