import axios from 'axios'
import { deleteCookie, getCookie } from './cookies'

const axiosInstance = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${getCookie('DBI_auth_token') || ''}`
  },
  withCredentials: true,
  timeout: 100000
})

axiosInstance.interceptors.request.use(
  (config) => {
    const accessToken = getCookie('DBI_auth_token')

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
      deleteCookie('DBI_auth_token')

      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export default axiosInstance
