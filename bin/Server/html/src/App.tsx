import { RouterProvider } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'

import { router } from './router.tsx'
// import { AuthProvider } from './AuthContext.tsx'

const queryClient = new QueryClient()

function App() {
  return (
    // <AuthProvider>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
    // </AuthProvider>
  )
}

export default App
