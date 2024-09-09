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

export default axiosInstance
