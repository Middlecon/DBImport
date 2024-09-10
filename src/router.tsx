import { createBrowserRouter } from 'react-router-dom'
import LogIn from './routes/LogIn'
import MainLayout from './components/MainLayout'
import Export from './routes/Export'
import Home from './routes/Home'
import Import from './routes/Import'
import DbTable from './components/DbTable'

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
            path: '/import/:db',
            element: <DbTable />
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
