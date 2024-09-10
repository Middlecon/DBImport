import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import LogIn from './routes/LogIn.tsx'
import Home from './routes/Home.tsx'
import PrivateRoute from './components/PrivateRoute.tsx'
import MainLayout from './components/MainLayout.tsx'
import Import from './routes/Import.tsx'
import Export from './routes/Export.tsx'

const queryClient = new QueryClient()

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

  return (
    <AuthProvider>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
    </AuthProvider>
  )
}

export default App
