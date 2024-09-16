import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { useParams } from 'react-router-dom'
import { Database, Table, UITable } from './interfaces'
import { mapDisplayValue } from './nameMappings'

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

export const useDbTables = (): UseQueryResult<UITable[], Error> => {
  const { db } = useParams<{ db: string }>()

  return useQuery({
    queryKey: ['tables', db],
    queryFn: async () => {
      if (!db) {
        return []
      }

      const data: Table[] = await getDbTables(db)

      return data.map(
        (row: {
          etlPhaseType: string
          importPhaseType: string
          importTool: string
          etlEngine: string
        }) => ({
          ...row,
          etlPhaseTypeDisplay: mapDisplayValue(
            'etlPhaseType',
            row.etlPhaseType
          ),
          importPhaseTypeDisplay: mapDisplayValue(
            'importPhaseType',
            row.importPhaseType
          ),
          importToolDisplay: mapDisplayValue('importTool', row.importTool),
          etlEngineDisplay: mapDisplayValue('etlEngine', row.etlEngine)
        })
      )
    },
    enabled: !!db
  })
}
