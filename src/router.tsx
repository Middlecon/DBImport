import { createBrowserRouter, Navigate } from 'react-router-dom'
import LogIn from './routes/LogIn'
import MainLayout from './components/MainLayout'
// import Export from './routes/Export'
import Home from './routes/Home'
import Import from './routes/import/Import'
import DbTables from './routes/import/DbTables'
import TableDetailedView from './routes/import/tableDetailed/TableDetailedView'
import TableSettings from './routes/import/tableDetailed/settings/TableSettings'
import TableColumns from './routes/import/tableDetailed/columns/TableColumns'
import TableStatistics from './routes/import/tableDetailed/statistics/TableStatistics'
import Connection from './routes/connection/Connection'
import ConnectionDetailedView from './routes/connection/connectionDetailed/ConnectionDetailedView'
import { AuthWrapper } from './authWrapper'

export const router = createBrowserRouter([
  {
    path: '/login',
    element: (
      <AuthWrapper>
        <LogIn />
      </AuthWrapper>
    )
    // loader: rootLoader,
  },
  {
    path: '/',
    element: (
      <AuthWrapper>
        <MainLayout />
      </AuthWrapper>
    ),

    children: [
      {
        path: '/',
        element: <Home />
      },
      {
        path: '/import',
        element: <Import />,
        children: [
          {
            path: ':database',
            element: <DbTables />
          }
        ]
      },
      {
        path: '/import/:database/:table',
        element: <TableDetailedView />,
        children: [
          {
            path: 'settings',
            element: <TableSettings />
          },
          {
            path: 'columns',
            element: <TableColumns />
          },
          {
            path: 'statistics',
            element: <TableStatistics />
          }
        ]
      },
      {
        path: '/export',
        element: <div>Export</div>
        // element: <Export />
      },
      {
        path: '/airflow/import',
        element: <div>Airflow Import</div>
      },
      {
        path: '/airflow/export',
        element: <div>Airflow Export</div>
      },
      {
        path: '/airflow/custom',
        element: <div>Airflow Custom</div>
      },
      {
        path: '/connection',
        element: <Connection />
      },
      {
        path: '/connection/:connection',
        element: <ConnectionDetailedView />
      },
      {
        path: '/configuration',
        element: <div>Configuration</div>
      },
      // Wildcard route to catch unmatched paths
      {
        path: '*',
        element: <Navigate to="/" replace />
      }
    ]
  }
])
