import { createBrowserRouter } from 'react-router-dom'
import LogIn from './routes/LogIn'
import MainLayout from './components/MainLayout'
import Export from './routes/Export'
import Home from './routes/Home'
import Import from './routes/import/Import'
import DbTables from './routes/import/DbTables'
import TableDetailedView from './routes/import/TableDetailedView'
import TableSettings from './routes/import/TableSettings'
import TableColumns from './routes/import/TableColumns'
import TableStatistics from './routes/import/TableStatistics'

export const router = createBrowserRouter([
  {
    path: '/login',
    element: <LogIn />
    // loader: rootLoader,
  },
  {
    path: '/',
    element: <MainLayout />,

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
        element: <Export />
      }
    ]
  }
])
