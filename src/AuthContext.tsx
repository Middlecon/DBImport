import { createContext, useEffect, useState } from 'react'
import { deleteCookie, getCookie } from './utils/cookies'
import axiosInstance from './utils/axiosInstance'

interface AuthContextType {
  isAuthenticated: boolean
  logout: () => void
}

export const AuthContext = createContext<AuthContextType>({
  isAuthenticated: false,
  logout: () => {}
})

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({
  children
}) => {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(
    !!getCookie('DBI_auth_token')
  )

  useEffect(() => {
    const interceptor = axiosInstance.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response && error.response.status === 401) {
          logout()
        }
        return Promise.reject(error)
      }
    )

    return () => {
      axiosInstance.interceptors.response.eject(interceptor)
    }
  }, [])

  const logout = () => {
    deleteCookie('DBI_auth_token')
    setIsAuthenticated(false)
    window.location.href = '/login' // Redirect to login
  }

  return (
    <AuthContext.Provider value={{ isAuthenticated, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

// export const useAuth = () => useContext(AuthContext);
