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
import AirflowImport from './routes/airflowImport/AirflowImport'
import AirflowExport from './routes/airflowExport/AirflowExport'
import AirflowCustom from './routes/airflowCustom/AirflowCustom'
import AirflowTasks from './routes/_airflowShared/AirflowTasks'
import AirflowDetailedView from './routes/_airflowShared/AirflowDetailedView'
import AirflowSettings from './routes/_airflowShared/AirflowSettings'

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
        element: <AirflowImport />
      },
      {
        path: '/airflow/import/:dagName',
        element: <AirflowDetailedView type="import" />,
        children: [
          {
            path: 'settings',
            element: <AirflowSettings type="import" />
          },
          {
            path: 'tasks',
            element: <AirflowTasks type="import" />
          }
        ]
      },
      {
        path: '/airflow/export',
        element: <AirflowExport />
      },
      {
        path: '/airflow/export/:dagName',
        element: <AirflowDetailedView type="export" />,
        children: [
          {
            path: 'settings',
            element: <AirflowSettings type="export" />
          },
          {
            path: 'tasks',
            element: <AirflowTasks type="export" />
          }
        ]
      },
      {
        path: '/airflow/custom',
        element: <AirflowCustom />
      },
      {
        path: '/airflow/custom/:dagName',
        element: <AirflowDetailedView type="custom" />,
        children: [
          {
            path: 'settings',
            element: <AirflowSettings type="custom" />
          },
          {
            path: 'tasks',
            element: <AirflowTasks type="custom" />
          }
        ]
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
