import { UseQueryResult, useQuery, useQueryClient } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { useParams } from 'react-router-dom'
import { Database, Table } from './interfaces'

// GET DATABASES

const getDatabases = async () => {
  const response = await axiosInstance.get('/import/db')
  return response.data
}

export const useDatabases = (): UseQueryResult<Database[], Error> => {
  return useQuery({
    queryKey: ['databases'],
    queryFn: getDatabases
  })
}

// GET TABLES FOR SPECIFIC DATABASE

const getDbTables = async (database: string) => {
  const response = await axiosInstance.get(`/import/table/${database}`)
  return response.data
}

export const useDbTables = (): UseQueryResult<Table[], Error> => {
  const { db } = useParams<{ db: string }>()
  const queryClient = useQueryClient()

  queryClient.invalidateQueries({ queryKey: ['tables'] })

  return useQuery({
    queryKey: ['tables', db],
    queryFn: () => {
      if (!db) {
        return Promise.resolve([])
      }
      return getDbTables(db)
    },
    enabled: !!db
  })
}
