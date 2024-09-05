import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import LogIn from './routes/LogIn.tsx'
import Home from './routes/Home.tsx'
import PrivateRoute from './components/PrivateRoute.tsx'
// import PrivateRoute from './components/PrivateRoute.tsx'

function App() {
  const router = createBrowserRouter([
    {
      path: '/login',
      element: <LogIn />
      // loader: rootLoader,
    },
    {
      path: '/',
      element: <PrivateRoute element={<Home />} />
      // loader: teamLoader,
    }
  ])

  return <RouterProvider router={router} />
}

export default App
