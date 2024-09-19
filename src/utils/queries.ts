import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { useParams } from 'react-router-dom'
import { Database, DbTable, Table, UiDbTable } from './interfaces'
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

// GET DATABASE TABLES

const getDbTables = async (database: string) => {
  const response = await axiosInstance.get(`/import/table/${database}`)
  return response.data
}

export const useDbTables = (): UseQueryResult<UiDbTable[], Error> => {
  const { database } = useParams<{ database: string }>()

  return useQuery({
    queryKey: ['tables', database],
    queryFn: async () => {
      if (!database) {
        throw new Error(
          'Can not fetch database tables because database params is not defined'
        )
      }

      const data: DbTable[] = await getDbTables(database)

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
    enabled: !!database
  })
}

// GET TABLE

const getTable = async (database: string, table: string) => {
  const response = await axiosInstance.get(`/import/table/${database}/${table}`)
  return response.data
}

export const useTable = (): UseQueryResult<Table, Error> => {
  const { database, table } = useParams<{ database: string; table: string }>()

  return useQuery({
    queryKey: ['table', table],
    queryFn: async () => {
      if (!database || !table) {
        throw new Error(
          'Can not fetch table because database or/and table params is not defined'
        )
      }

      const data: Table = await getTable(database, table)

      return data
    },
    enabled: !!database && !!table
  })
}
