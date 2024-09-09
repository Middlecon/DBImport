import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'

interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
}

const getDatabases = async () => {
  const response = await axiosInstance.get('/import/db')
  console.log('databases', response.data)

  return response.data
}

export const useDatabases = (): UseQueryResult<Database[], Error> => {
  return useQuery({
    queryKey: ['databases'],
    queryFn: getDatabases
  })
}
