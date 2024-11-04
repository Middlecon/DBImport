import { Navigate, useLocation } from 'react-router-dom'
import { useEffect } from 'react'

export const AuthWrapper = ({ children }: { children: React.ReactNode }) => {
  const location = useLocation()

  const isAuthenticated = () => {
    return document.cookie.includes('DBI_auth_token')
  }

  const isAuth = isAuthenticated()

  useEffect(() => {
    if (isAuth || location.pathname === '/login') {
      document.body.classList.remove('cloak')
    }
  }, [isAuth, location.pathname])

  if (!isAuth && location.pathname !== '/login') {
    return <Navigate to="/login" replace />
  }

  return <>{children}</>
}
