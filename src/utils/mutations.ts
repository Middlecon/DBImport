import { useMutation } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import { TableCreateUpdate } from './interfaces'

const postTable = async (table: TableCreateUpdate) => {
  console.log('postTable table', table)
  const response = await axiosInstance.post('/import/table', table)
  console.log('postTable response.data', response.data)

  return response.data
}

export const useUpdateTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: TableCreateUpdate) => {
      return postTable(tableUpdated)
    }
  })
}
