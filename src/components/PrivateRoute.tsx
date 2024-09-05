import { Navigate } from 'react-router-dom'

type PrivateRouteProps = {
  element: React.ReactElement
}

const PrivateRoute: React.FC<PrivateRouteProps> = ({ element }) => {
  const authToken = sessionStorage.getItem('DBI_auth_token')

  if (!authToken) {
    // If no token is present, redirect to login
    return <Navigate to="/login" replace />
  }

  // If token exists, render the given element
  return element
}

export default PrivateRoute
