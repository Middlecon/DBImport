import { useMutation } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  TableCreateWithoutEnum,
  Connection,
  UITableWithoutEnum,
  CustomAirflowDAG,
  ExportAirflowDAG,
  ImportAirflowDAG,
  CustomCreateAirflowDAG,
  ExportCreateAirflowDAG,
  ImportCreateAirflowDAG,
  UIExportTableWithoutEnum,
  ExportTableCreateWithoutEnum,
  ConfigGlobalWithIndex,
  JDBCdriversWithIndex,
  BulkUpdateExportTables,
  BulkUpdateImportTables,
  BulkUpdateAirflowDAG
} from './interfaces'

// Connection

const postUpdateConnection = async (connection: Connection) => {
  console.log('postUpdateConnection connection', connection)
  const response = await axiosInstance.post('/connection', connection)
  console.log('postUpdateConnection response.data', response.data)

  return response.data
}

export const useUpdateConnection = () => {
  return useMutation({
    mutationFn: (connectionUpdated: Connection) => {
      return postUpdateConnection(connectionUpdated)
    }
  })
}

type DeleteConnectionArgs = {
  connectionName: string
}

const deleteConnection = async ({ connectionName }: DeleteConnectionArgs) => {
  const encodedConnection = encodeURIComponent(connectionName)

  const response = await axiosInstance.delete(
    `/connection/${encodedConnection}`
  )
  return response.data
}

export const useDeleteConnection = () => {
  return useMutation({
    mutationFn: (args: DeleteConnectionArgs) => deleteConnection(args)
  })
}

// Update table, Import or Export

const postBulkUpdateTable = async (
  type: 'import' | 'export',
  bulkUpdateJson: BulkUpdateImportTables | BulkUpdateExportTables
) => {
  console.log('bulkUpdateJson', bulkUpdateJson)
  const response = await axiosInstance.post(`/${type}/table`, bulkUpdateJson)
  console.log('postBulkUpdateTable response.data', response.data)

  return response.data
}

interface BulkPostTableProps {
  type: 'import' | 'export'
  bulkUpdateJson: BulkUpdateImportTables | BulkUpdateExportTables
}

export const useBulkUpdateTable = () => {
  return useMutation<
    unknown, // Return type of mutation function, specify it more?
    Error,
    BulkPostTableProps
  >({
    mutationFn: ({ type, bulkUpdateJson }) => {
      return postBulkUpdateTable(type, bulkUpdateJson)
    }
  })
}

// Update table, Import or Export

const postUpdateTable = async (
  type: 'import' | 'export',
  table: UITableWithoutEnum | UIExportTableWithoutEnum
) => {
  const { database, table: tableName, ...importTableObject } = table
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(tableName)

  const { connection, targetSchema, targetTable, ...exportTableObject } = table
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema as string)
  const encodedTargetTable = encodeURIComponent(targetTable as string)

  const postUrl =
    type === 'import'
      ? `/${type}/table/${encodedDatabase}/${encodedTable}`
      : `/${type}/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`

  const tableObject = type === 'import' ? importTableObject : exportTableObject

  console.log('postTable type', type)
  console.log('postTable table', table)
  const response = await axiosInstance.post(postUrl, tableObject)
  console.log('postTable response.data', response.data)

  return response.data
}

interface PostTableProps {
  type: 'import' | 'export'
  table: UITableWithoutEnum | UIExportTableWithoutEnum
}

export const useUpdateTable = () => {
  return useMutation<
    unknown, // Return type of mutation function, specify it more?
    Error,
    PostTableProps
  >({
    mutationFn: ({ type, table }) => {
      return postUpdateTable(type, table)
    }
  })
}

// Import create table

const postCreateImportTable = async (table: TableCreateWithoutEnum) => {
  console.log('postTable table', table)

  const { database, table: tableName, ...tableObject } = table
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(tableName)

  const response = await axiosInstance.post(
    `/import/table/${encodedDatabase}/${encodedTable}`,
    tableObject
  )
  console.log('postTable response.data', response.data)
  return response.data
}

export const useCreateImportTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: TableCreateWithoutEnum) => {
      return postCreateImportTable(tableUpdated)
    }
  })
}

// Import delete table

type DeleteImportTableArgs = {
  database: string
  table: string
}

const deleteImportTable = async ({
  database,
  table
}: DeleteImportTableArgs) => {
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(table)

  const response = await axiosInstance.delete(
    `/import/table/${encodedDatabase}/${encodedTable}`
  )
  return response.data
}

export const useDeleteImportTable = () => {
  return useMutation({
    mutationFn: (args: DeleteImportTableArgs) => deleteImportTable(args)
  })
}

// Export create table

const postCreateExportTable = async (table: ExportTableCreateWithoutEnum) => {
  const { connection, targetSchema, targetTable, ...exportTableObject } = table
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema)
  const encodedTargetTable = encodeURIComponent(targetTable)

  const response = await axiosInstance.post(
    `/export/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`,
    exportTableObject
  )
  console.log('postTable response.data', response.data)

  return response.data
}

export const useCreateExportTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: ExportTableCreateWithoutEnum) => {
      return postCreateExportTable(tableUpdated)
    }
  })
}

// Export delete table

type DeleteExportTableArgs = {
  connection: string
  targetSchema: string
  targetTable: string
}

const deleteExportTable = async ({
  connection,
  targetSchema,
  targetTable
}: DeleteExportTableArgs) => {
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema)
  const encodedTargetTable = encodeURIComponent(targetTable)

  const response = await axiosInstance.delete(
    `/export/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`
  )
  return response.data
}

export const useDeleteExportTable = () => {
  return useMutation({
    mutationFn: (args: DeleteExportTableArgs) => deleteExportTable(args)
  })
}

// Airflow

const updateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
) => {
  const { name } = dagData
  const encodedDagName = encodeURIComponent(name)

  const response = await axiosInstance.post(
    `/airflow/dags/${type}/${encodedDagName}`,
    dagData
  )
  return response.data
}

export const useUpdateAirflowDag = () => {
  return useMutation<
    ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => updateAirflowDag(type, dagData)
  })
}

const postCreateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData:
    | ImportCreateAirflowDAG
    | ExportCreateAirflowDAG
    | CustomCreateAirflowDAG
) => {
  const { name, ...dagDataObject } = dagData
  const encodedDagName = encodeURIComponent(name)

  const response = await axiosInstance.post(
    `/airflow/dags/${type}/${encodedDagName}`,
    dagDataObject
  )
  return response.data
}

export const useCreateAirflowDag = () => {
  return useMutation<
    ImportCreateAirflowDAG | ExportCreateAirflowDAG | CustomCreateAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData:
        | ImportCreateAirflowDAG
        | ExportCreateAirflowDAG
        | CustomCreateAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => postCreateAirflowDag(type, dagData)
  })
}

// Airflow delete DAG

type DeleteAirflowDAGArgs = {
  type: 'import' | 'export' | 'custom'
  dagName: string
}

const deleteAirflowDAG = async ({ type, dagName }: DeleteAirflowDAGArgs) => {
  const encodedDagName = encodeURIComponent(dagName)
  const response = await axiosInstance.delete(
    `/airflow/dags/${type}/${encodedDagName}`
  )
  return response.data
}

export const useDeleteAirflowDAG = () => {
  return useMutation({
    mutationFn: (args: DeleteAirflowDAGArgs) => deleteAirflowDAG(args)
  })
}

// Bulk update Airflow DAG

const postBulkUpdateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData: BulkUpdateAirflowDAG
) => {
  const response = await axiosInstance.post(`/airflow/dags/${type}`, dagData)
  return response.data
}

export const useBulkUpdateAirflowDag = () => {
  return useMutation<
    BulkUpdateAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData: BulkUpdateAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => postBulkUpdateAirflowDag(type, dagData)
  })
}

// Configuration

const postGlobalConfig = async (config: ConfigGlobalWithIndex) => {
  console.log('post config:', config)
  const response = await axiosInstance.post('/config/updateConfig', config)
  console.log('post config response.data', response.data)

  return response.data
}

export const useUpdateGlobalConfig = () => {
  return useMutation({
    mutationFn: (globalConfigUpdated: ConfigGlobalWithIndex) => {
      return postGlobalConfig(globalConfigUpdated)
    }
  })
}

const postJDBCdriver = async (driver: JDBCdriversWithIndex) => {
  console.log('post driver:', driver)
  const response = await axiosInstance.post('/config/updateJDBCdriver', driver)
  console.log('post driver response.data', response.data)

  return response.data
}

export const useUpdateJDBCdrivers = () => {
  return useMutation({
    mutationFn: (driverUpdated: JDBCdriversWithIndex) => {
      return postJDBCdriver(driverUpdated)
    }
  })
}
