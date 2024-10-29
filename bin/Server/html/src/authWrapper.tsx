import { Navigate, useLocation } from 'react-router-dom'
import { useEffect } from 'react'
import { isAirflowSubmenuActiveAtom } from './atoms/atoms'
import { useAtom } from 'jotai'

export const AuthWrapper = ({ children }: { children: React.ReactNode }) => {
  const location = useLocation()
  const [, setIsAirflowSubmenuActive] = useAtom(isAirflowSubmenuActiveAtom)

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
    setIsAirflowSubmenuActive(false)
    return <Navigate to="/login" replace />
  }

  return <>{children}</>
}
