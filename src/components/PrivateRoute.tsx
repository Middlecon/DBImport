import { Navigate } from 'react-router-dom'
import { getCookie } from '../utils/cookies'

type PrivateRouteProps = {
  element: React.ReactElement
}

const PrivateRoute: React.FC<PrivateRouteProps> = ({ element }) => {
  const authToken = getCookie('DBI_auth_token')

  if (!authToken) {
    return <Navigate to="/login" replace />
  }
  return element
}

export default PrivateRoute
