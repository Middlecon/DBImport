import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import LogIn from './routes/LogIn.tsx'
import Home from './routes/Home.tsx'
import PrivateRoute from './components/PrivateRoute.tsx'
import MainLayout from './components/MainLayout.tsx'
import Import from './routes/Import.tsx'
import Export from './routes/Export.tsx'

function App() {
  const router = createBrowserRouter([
    {
      path: '/login',
      element: <LogIn />
      // loader: rootLoader,
    },
    {
      path: '/',
      element: <PrivateRoute element={<MainLayout />} />,
      children: [
        {
          path: '/',
          element: <Home />
        },
        {
          path: '/import',
          element: <Import />
        },
        {
          path: '/export',
          element: <Export />
        }
      ]
    }
  ])

  return <RouterProvider router={router} />
}

export default App
