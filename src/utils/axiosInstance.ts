import axios from 'axios'
import { getCookie } from './cookies'

const axiosInstance = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${getCookie('DBI_auth_token') || ''}`
  },
  withCredentials: true,
  timeout: 10000
})

axiosInstance.interceptors.request.use(
  (config) => {
    const accessToken = getCookie('DBI_auth_token')
    console.log('accessToken', accessToken)

    if (accessToken) {
      config.headers.Authorization = `Bearer ${accessToken}`
    }
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

axiosInstance.interceptors.response.use(
  (response) => response,
  (error) => {
    console.log('error.response AXIOS', error.response)

    if (error.response && error.response.status === 401) {
      document.cookie =
        'DBI_auth_token=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;'

      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export default axiosInstance
