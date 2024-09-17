import { createBrowserRouter } from 'react-router-dom'
import LogIn from './routes/LogIn'
import MainLayout from './components/MainLayout'
import Export from './routes/Export'
import Home from './routes/Home'
import Import from './routes/import/Import'
import DbTables from './routes/import/DbTables'
import TableDetailedView from './routes/import/TableDetailedView'

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
        element: <TableDetailedView />
      },
      {
        path: '/export',
        element: <Export />
      }
    ]
  }
])
