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
  JDBCdriversWithIndex
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

// Table

const postUpdateTable = async (
  type: 'import' | 'export',
  table: UITableWithoutEnum | UIExportTableWithoutEnum
) => {
  console.log('postTable type', type)
  console.log('postTable table', table)
  const response = await axiosInstance.post(`/${type}/table`, table)
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

// Import table

const postCreateImportTable = async (table: TableCreateWithoutEnum) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/import/table', table)
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

// Export Table

const postCreateExportTable = async (table: ExportTableCreateWithoutEnum) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/export/table', table)
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

// Airflow

const updateAirflowDag = async (
  type: string,
  dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
) => {
  const response = await axiosInstance.post(`/airflow/dags/${type}`, dagData)
  return response.data
}

export const useUpdateAirflowDag = () => {
  return useMutation<
    ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG,
    Error,
    {
      type: string
      dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => updateAirflowDag(type, dagData)
  })
}

const postCreateAirflowDag = async (
  type: string,
  dagData:
    | ImportCreateAirflowDAG
    | ExportCreateAirflowDAG
    | CustomCreateAirflowDAG
) => {
  const response = await axiosInstance.post(`/airflow/dags/${type}`, dagData)
  return response.data
}

export const useCreateAirflowDag = () => {
  return useMutation<
    ImportCreateAirflowDAG | ExportCreateAirflowDAG | CustomCreateAirflowDAG,
    Error,
    {
      type: string
      dagData:
        | ImportCreateAirflowDAG
        | ExportCreateAirflowDAG
        | CustomCreateAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => postCreateAirflowDag(type, dagData)
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
