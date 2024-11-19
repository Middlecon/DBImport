import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  AirflowsCustomData,
  AirflowsData,
  AirflowsExportData,
  AirflowsImportData,
  ConfigGlobal,
  Connection,
  Connections,
  CustomAirflowDAG,
  Database,
  DbTable,
  ExportAirflowDAG,
  ExportCnTable,
  ExportConnections,
  ExportTable,
  ImportAirflowDAG,
  JDBCdrivers,
  Table,
  UIExportCnTables,
  UIExportTable,
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
  ExportIncrValidationMethod,
  ExportTool,
  ExportType,
  ExportValidationMethod,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  ValidateSource,
  ValidationMethod
} from './enums'
import {
  AxiosResponseHeaders,
  AxiosHeaderValue,
  RawAxiosResponseHeaders
} from 'axios'

// CONNECTION

// Get all connections

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

// Get connection

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

// IMPORT

// Get databases for databases dropdown

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

// Get search filter import tables

interface ImportTablesResponse {
  data: DbTable[]
  headers:
    | AxiosResponseHeaders
    | Partial<
        RawAxiosResponseHeaders & {
          Server: AxiosHeaderValue
          [key: string]: AxiosHeaderValue // Dynamic headers
        }
      >
}

const getSearchImportTables = async (
  database: string | null,
  table: string | null
) => {
  const response = await axiosInstance.post('/import/search', {
    database,
    table
  })
  console.log('response.data', response.data)
  return {
    data: response.data,
    headers: response.headers
  }
}

export const useSearchImportTables = (
  database: string | null,
  table: string | null
): UseQueryResult<UiDbTable[], Error> => {
  console.log('useSearchImportTables database', database)
  console.log('useSearchImportTables table', table)
  return useQuery({
    queryKey: ['import', 'search', database, table],
    queryFn: async () => {
      const { data, headers }: ImportTablesResponse =
        await getSearchImportTables(database, table)
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
      console.log('Headers:', headers)

      return enumMappedData
    },
    refetchOnWindowFocus: false
  })
}

// Get detailed table

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
    queryKey: ['import', database, table],
    queryFn: () => fetchTableData(database!, table!), // We are sure that database and table is not null here because of the enabled flag
    enabled: !!database && !!table
  })
}

// EXPORT

// Get connections with exports

const getExportConnections = async () => {
  const response = await axiosInstance.get('/export/connection')
  console.log('response.data', response.data)
  return response.data
}

export const useExportConnections = (): UseQueryResult<
  ExportConnections[],
  Error
> => {
  return useQuery({
    queryKey: ['export', 'connections'],
    queryFn: getExportConnections,
    initialData: []
  })
}

// Get export tables of a connection

interface ExportTablesResponse {
  data: ExportCnTable[]
  headers:
    | AxiosResponseHeaders
    | Partial<
        RawAxiosResponseHeaders & {
          Server: AxiosHeaderValue
          [key: string]: AxiosHeaderValue // Dynamic headers
        }
      >
}

const getSearchExportTables = async (
  connection: string | null,
  targetSchema: string | null,
  targetTable: string | null
) => {
  const response: ExportTablesResponse = await axiosInstance.post(
    '/export/search',
    {
      connection,
      targetSchema,
      targetTable
    }
  )
  console.log('response.data', response.data)
  return {
    data: response.data,
    headers: response.headers
  }
}

export const useSearchExportTables = (
  connection: string | null,
  targetSchema: string | null,
  targetTable: string | null
): UseQueryResult<UIExportCnTables[], Error> => {
  return useQuery({
    queryKey: ['export', 'search', connection, targetSchema, targetTable],
    queryFn: async () => {
      const { data, headers }: ExportTablesResponse =
        await getSearchExportTables(connection, targetSchema, targetTable)

      const enumMappedData = data.map((row) => ({
        ...row,
        exportTypeDisplay: mapDisplayValue(
          'exportType',
          row.exportType as string
        ),
        exportToolDisplay: mapDisplayValue(
          'exportTool',
          row.exportTool as string
        )
      }))

      console.log('Headers:', headers)

      return enumMappedData
    },
    refetchOnWindowFocus: false
  })
}

// Get detailed export table

const getExportTable = async (
  connection: string,
  schema: string,
  table: string
) => {
  const response = await axiosInstance.get(
    `/export/table/${connection}/${schema}/${table}`
  )
  return response.data
}

export const fetchExportTableData = async (
  connection: string,
  schema: string,
  table: string
): Promise<UIExportTable> => {
  const data: ExportTable = await getExportTable(connection, schema, table)

  const dataWithEnumTypes: UIExportTable = {
    ...data,
    exportType: mapEnumValue(data.exportType, Object.values(ExportType)),
    exportTool: mapEnumValue(data.exportTool, Object.values(ExportTool)),
    validationMethod:
      typeof data.validationMethod === 'string'
        ? mapEnumValue(
            data.validationMethod,
            Object.values(ExportValidationMethod)
          )
        : null,
    incrValidationMethod:
      typeof data.incrValidationMethod === 'string'
        ? mapEnumValue(
            data.incrValidationMethod,
            Object.values(ExportIncrValidationMethod)
          )
        : null
  }

  return dataWithEnumTypes
}

export const useExportTable = (
  connection?: string,
  schema?: string,
  table?: string
): UseQueryResult<UIExportTable, Error> => {
  return useQuery({
    queryKey: ['export', connection, table],
    queryFn: () => fetchExportTableData(connection!, schema!, table!), // We are sure that database and table is not null here because of the enabled flag
    enabled: !!connection && !!schema && !!table
  })
}

// AIRFLOW

// Get all airflows

const getAllAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags')
  console.log('getAirflows response.data', response.data)
  return response.data
}

export const useAllAirflows = (): UseQueryResult<AirflowsData[], Error> => {
  return useQuery({
    queryKey: ['airflows'],
    queryFn: getAllAirflows,
    initialData: []
  })
}

// // Get all type airflows

// const getAirflows = async (type: string) => {
//   const response = await axiosInstance.get(`/airflow/dags/${type}`)
//   console.log('getAirflows response.data', response.data)
//   return response.data
// }

// export const useAirflows = (
//   type?: string
// ): UseQueryResult<
//   UiAirflowsImportData[] | UiAirflowsExportData[] | UiAirflowsCustomData[],
//   Error
// > => {
//   return useQuery({
//     queryKey: ['airflows', type],
//     queryFn: async () => {
//       const data:
//         | AirflowsImportData[]
//         | AirflowsExportData[]
//         | AirflowsCustomData[] = await getAirflows(type!) // We are sure that type is not null here because of the enabled flag
//       const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
//         ...row,
//         autoRegenerateDagDisplay:
//           row.autoRegenerateDag === true ? 'True' : 'False'
//       }))

//       return mappedData
//     },
//     enabled: !!type
//   })
// }

// Get all import airflows

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

// Get all export airflows

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

// Get all custom airflows

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

// Get an airlow DAG

const getAirflowDAG = async (type: string, dagName: string) => {
  const response = await axiosInstance.get(`/airflow/dags/${type}/${dagName}`)
  return response.data
}

export const useAirflowDAG = (
  type?: string,
  dagName?: string
): UseQueryResult<
  ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG,
  Error
> => {
  return useQuery({
    queryKey: ['airflows', type, dagName],
    queryFn: () => getAirflowDAG(type!, dagName!), // We are sure that type and dagName is not null here because of the enabled flag
    enabled: !!type && !!dagName
  })
}

// VERSION

// Get server status and version

const getStatusAndVersion = async () => {
  const response = await axiosInstance.get('/status')
  return response.data
}

export const useGetStatusAndVersion = (): UseQueryResult<
  { status: string; version: string },
  Error
> => {
  return useQuery({
    queryKey: ['status'],
    queryFn: () => getStatusAndVersion()
  })
}

// CONFIGURATION

const getGlobalConfig = async () => {
  const response = await axiosInstance.get('/config/getConfig')
  return response.data
}

export const useGlobalConfig = (): UseQueryResult<ConfigGlobal, Error> => {
  return useQuery({
    queryKey: ['configuration', 'global'],
    queryFn: () => getGlobalConfig()
  })
}

const getJDBCdrivers = async () => {
  const response = await axiosInstance.get('/config/getJDBCdrivers')
  return response.data
}

export const useJDBCDrivers = (): UseQueryResult<JDBCdrivers[], Error> => {
  return useQuery({
    queryKey: ['configuration', 'jdbcdrivers'],
    queryFn: () => getJDBCdrivers()
  })
}
