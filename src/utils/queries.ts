import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  AirflowsCustomData,
  AirflowsExportData,
  AirflowsImportData,
  Connection,
  Connections,
  CustomAirflowDAG,
  Database,
  DbTable,
  ExportAirflowDAG,
  ImportAirflowDAG,
  Table,
  UITable,
  UiAirflowsCustomData,
  UiAirflowsExportData,
  UiAirflowsImportData,
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

// GET ALL CONNECTIONS

const getConnections = async (onlyNames?: boolean) => {
  const path =
    onlyNames === true ? '/connection?listOnlyName=true' : '/connection'
  const response = await axiosInstance.get(path)
  return response.data
}

export const useConnections = (
  onlyNames?: boolean
): UseQueryResult<Connections[], Error> => {
  const queryKey =
    onlyNames === true ? ['connections', 'names'] : ['connections']
  return useQuery({
    queryKey: queryKey,
    queryFn: () => getConnections(onlyNames)
  })
}

// GET CONNECTION

const getConnection = async (connection: string) => {
  const response = await axiosInstance.get(`/connection/${connection}`)
  return response.data
}

export const useConnection = (
  connection?: string
): UseQueryResult<Connection, Error> => {
  return useQuery({
    queryKey: ['connection', connection],
    queryFn: () => getConnection(connection!), // We are sure that connection is not null here because of the enabled flag
    enabled: !!connection
  })
}

// GET DATABASES

const getDatabases = async () => {
  const response = await axiosInstance.get('/import/db')
  console.log('response.data', response.data)
  return response.data
}

export const useDatabases = (): UseQueryResult<Database[], Error> => {
  return useQuery({
    queryKey: ['databases'],
    queryFn: getDatabases,
    initialData: []
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
      const enumMappedData = data.map(
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

      return enumMappedData
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
    queryFn: () => fetchTableData(database!, table!), // We are sure that database and table is not null here because of the enabled flag
    enabled: !!database && !!table
  })
}

// GET ALL AIRFLOWS

const getAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags')
  console.log('getAirflows response.data', response.data)
  return response.data
}

export const useAirflows = (): UseQueryResult<[], Error> => {
  return useQuery({
    queryKey: ['airflows'],
    queryFn: getAirflows,
    initialData: []
  })
}

// GET ALL IMPORT AIRFLOWS

const getImportAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/import')
  console.log('getImportAirflows response.data', response.data)
  return response.data
}

export const useImportAirflows = (): UseQueryResult<
  UiAirflowsImportData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'import'],
    queryFn: async () => {
      const data: AirflowsImportData[] = await getImportAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    }
  })
}

// GET ALL EXPORT AIRFLOWS

const getExportAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/export')
  console.log('getExportAirflows response.data', response.data)
  return response.data
}

export const useExportAirflows = (): UseQueryResult<
  UiAirflowsExportData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'export'],
    queryFn: async () => {
      const data: AirflowsExportData[] = await getExportAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    },
    initialData: [],
    refetchOnWindowFocus: false
  })
}

// GET ALL CUSTOM AIRFLOWS

const getCustomAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/custom')
  console.log('getCustomAirflows response.data', response.data)
  return response.data
}

export const useCustomAirflows = (): UseQueryResult<
  UiAirflowsCustomData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'custom'],
    queryFn: async () => {
      const data: AirflowsCustomData[] = await getCustomAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    },
    initialData: [],
    refetchOnWindowFocus: false
  })
}

// GET AN IMPORT AIRFLOW DAG

const getImportAirflowDAG = async (dagName: string) => {
  const response = await axiosInstance.get(`/airflow/dags/import/${dagName}`)
  return response.data
}

export const useImportAirflowDAG = (
  dagName?: string
): UseQueryResult<ImportAirflowDAG, Error> => {
  return useQuery({
    queryKey: ['airflows', 'import', dagName],
    queryFn: () => getImportAirflowDAG(dagName!), // We are sure that dagName is not null here because of the enabled flag
    enabled: !!dagName
  })
}

// GET AN EXPORT AIRFLOW DAG

const getExportAirflowDAG = async (dagName: string) => {
  const response = await axiosInstance.get(`/airflow/dags/export/${dagName}`)
  return response.data
}

export const useExportAirflowDAG = (
  dagName?: string
): UseQueryResult<ExportAirflowDAG, Error> => {
  return useQuery({
    queryKey: ['airflows', 'export', dagName],
    queryFn: () => getExportAirflowDAG(dagName!), // We are sure that dagName is not null here because of the enabled flag
    enabled: !!dagName
  })
}

// GET A CUSTOM AIRFLOW DAG

const getCustomAirflowDAG = async (dagName: string) => {
  const response = await axiosInstance.get(`/airflow/dags/custom/${dagName}`)
  return response.data
}

export const useCustomAirflowDAG = (
  dagName?: string
): UseQueryResult<CustomAirflowDAG, Error> => {
  return useQuery({
    queryKey: ['airflows', 'custom', dagName],
    queryFn: () => getCustomAirflowDAG(dagName!), // We are sure that dagName is not null here because of the enabled flag
    enabled: !!dagName
  })
}
