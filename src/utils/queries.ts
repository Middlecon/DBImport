import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { useParams } from 'react-router-dom'
import {
  Connection,
  Database,
  DbTable,
  Table,
  UITable,
  UiDbTable
} from './interfaces'
import { mapDisplayValue } from './nameMappings'
import {
  EtlEngine,
  EtlType,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  ValidateSource,
  ValidationMethod,
  mapEnumValue
} from './enums'

// GET CONNECTIONS

const getConnections = async () => {
  const response = await axiosInstance.get('/connection')
  return response.data
}

export const useConnections = (): UseQueryResult<Connection[], Error> => {
  return useQuery({
    queryKey: ['connections'],
    queryFn: getConnections
  })
}

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

export const fetchTableData = async (
  database: string,
  table: string
): Promise<UITable> => {
  if (!database || !table) {
    throw new Error(
      'Cannot fetch table because database or/and table params are not defined'
    )
  }

  const data: Table = await getTable(database, table)

  const dataWithEnumTypes: UITable = {
    ...data,
    importPhaseType: mapEnumValue(
      data.importPhaseType,
      Object.values(ImportType),
      'Unknown'
    ),
    etlPhaseType: mapEnumValue(
      data.etlPhaseType,
      Object.values(EtlType),
      'Unknown'
    ),
    importTool: mapEnumValue(
      data.importTool,
      Object.values(ImportTool),
      'Unknown'
    ),
    etlEngine: mapEnumValue(
      data.etlEngine,
      Object.values(EtlEngine),
      'Unknown'
    ),
    validationMethod: mapEnumValue(
      data.validationMethod,
      Object.values(ValidationMethod),
      'Unknown'
    ),
    validateSource: mapEnumValue(
      data.validateSource,
      Object.values(ValidateSource),
      'Unknown'
    ),
    incrMode: mapEnumValue(data.incrMode, Object.values(IncrMode), 'Unknown'),
    incrValidationMethod: mapEnumValue(
      data.incrValidationMethod,
      Object.values(IncrValidationMethod),
      'Unknown'
    ),
    mergeCompactionMethod: mapEnumValue(
      data.mergeCompactionMethod,
      Object.values(MergeCompactionMethod),
      'Unknown'
    )
  }

  return dataWithEnumTypes
}

export const useTable = (
  database?: string,
  table?: string
): UseQueryResult<UITable, Error> => {
  return useQuery({
    queryKey: ['table', table],
    queryFn: () => fetchTableData(database!, table!),
    enabled: !!database && !!table
  })
}
