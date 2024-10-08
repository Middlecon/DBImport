import { useMutation } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { TableUpdate, TableCreateWithoutEnum } from './interfaces'

const postUpdateTable = async (table: TableUpdate) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/import/table', table)
  console.log('postTable response.data', response.data)

  return response.data
}

export const useUpdateTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: TableUpdate) => {
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
