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
  ImportCreateAirflowDAG
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

const postUpdateTable = async (table: UITableWithoutEnum) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/import/table', table)
  console.log('postTable response.data', response.data)

  return response.data
}

export const useUpdateTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: UITableWithoutEnum) => {
      return postUpdateTable(tableUpdated)
    }
  })
}

const postCreateTable = async (table: TableCreateWithoutEnum) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/import/table', table)
  console.log('postTable response.data', response.data)

  return response.data
}

export const useCreateTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: TableCreateWithoutEnum) => {
      return postCreateTable(tableUpdated)
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
