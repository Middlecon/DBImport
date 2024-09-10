import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { useParams } from 'react-router-dom'

interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
}

interface Table {
  connection: string
  database: string
  etlEngine: string
  etlPhaseType: string
  importPhaseType: string
  importTool: string
  lastUpdateFromSource: string
  sourceSchema: string
  sourceTable: string
  table: string
}

// GET DATABASES

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

// GET TABLES FOR SPECIFIC DATABASE

const getDbTables = async (database: string) => {
  const response = await axiosInstance.get(`/import/table/${database}`)
  console.log('tables', response.data)

  return response.data
}

export const useDbTables = (): UseQueryResult<Table[], Error> => {
  const { db } = useParams<{ db: string }>()
  if (!db) {
    throw new Error('Database parameter is missing')
  }

  return useQuery({
    queryKey: ['tables'],
    queryFn: () => getDbTables(db)
  })
}
