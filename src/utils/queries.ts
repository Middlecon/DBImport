import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  Connection,
  Database,
  DbTable,
  Table,
  UITable,
  UiDbTable
} from './interfaces'
import { mapDisplayValue, mapEnumValue } from './nameMappings'
import {
  EtlEngine,
  EtlType,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  ValidateSource,
  ValidationMethod
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

export const useDbTables = (
  database: string | null
): UseQueryResult<UiDbTable[], Error> => {
  return useQuery({
    queryKey: ['tables', database],
    queryFn: async () => {
      const data: DbTable[] = await getDbTables(database!) // We are sure that database is not null here because of the enabled flag
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
    enabled: !!database,
    refetchOnWindowFocus: false
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
      Object.values(ImportType)
    ),
    etlPhaseType: mapEnumValue(data.etlPhaseType, Object.values(EtlType)),
    importTool: mapEnumValue(data.importTool, Object.values(ImportTool)),
    etlEngine: mapEnumValue(data.etlEngine, Object.values(EtlEngine)),
    validationMethod: mapEnumValue(
      data.validationMethod,
      Object.values(ValidationMethod)
    ),
    validateSource: mapEnumValue(
      data.validateSource,
      Object.values(ValidateSource)
    ),
    incrMode: mapEnumValue(data.incrMode, Object.values(IncrMode)),
    incrValidationMethod: mapEnumValue(
      data.incrValidationMethod,
      Object.values(IncrValidationMethod)
    ),
    mergeCompactionMethod: mapEnumValue(
      data.mergeCompactionMethod,
      Object.values(MergeCompactionMethod)
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
