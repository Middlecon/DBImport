import '../import/Import.scss'
import { useCallback, useMemo, useState } from 'react'
import { useJDBCDrivers } from '../../utils/queries'
import { Column, EditSetting, JDBCdrivers } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import EditTableModal from '../../components/EditTableModal'
import { JDBCdriversRowDataEdit } from '../../utils/cardRenderFormatting'
import { updateJDBCdriversData } from '../../utils/dataFunctions'
// import { useQueryClient } from '@tanstack/react-query'
// import { useUpdateJDBCdrivers } from '../../utils/mutations'

function ConfigJDBCDrivers() {
  // const queryClient = useQueryClient()
  // const { mutate: updateDriver } = useUpdateJDBCdrivers()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [currentRow, setCurrentRow] = useState<EditSetting[] | []>([])
  const [row, setRow] = useState<JDBCdrivers>()

  const { data: originalDriverData, isLoading } = useJDBCDrivers()

  const columns: Column<JDBCdrivers>[] = useMemo(
    () => [
      { header: 'Database Type', accessor: 'databaseType' },
      { header: 'Version', accessor: 'version' },
      { header: 'Driver', accessor: 'driver' },
      { header: 'Classpath', accessor: 'classpath' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: JDBCdrivers) => {
      if (!originalDriverData) {
        console.error('Drivers data is not available.')
        return
      }

      const rowData: EditSetting[] = JDBCdriversRowDataEdit(row)
      console.log('rowData', rowData)
      console.log('row', row)
      setRow(row)
      setCurrentRow(rowData)
      setIsEditModalOpen(true)
    },
    [originalDriverData]
  )

  if (!originalDriverData) return <div className="loading">Loading...</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    console.log('updatedSettings', updatedSettings)

    const originalDataCopy: JDBCdrivers[] = { ...originalDriverData }

    console.log('originalDataCopy', originalDataCopy)

    const editedJDBCDriversData = updateJDBCdriversData(
      row!, // row is set at edit click before user can save
      updatedSettings
    )

    console.log('editedJDBCDriversData', editedJDBCDriversData)

    // queryClient.setQueryData(
    //   ['configuration', 'jdbcdrivers'],
    //   editedJDBCDriversData
    // )
    // updateDriver(editedJDBCDriversData, {
    //   onSuccess: (response) => {
    //     queryClient.invalidateQueries({
    //       queryKey: ['configuration', 'jdbcdrivers']
    //     }) // For getting fresh data from database to the cache
    //     console.log('Update successful', response)
    //     setIsEditModalOpen(false)
    //   },
    //   onError: (error) => {
    // queryClient.setQueryData(
    //   ['configuration', 'jdbcdrivers'],
    //   originalDriverData
    // )

    //   console.error('Error updating JDBC Drivers', error)
    // }
    // })
  }

  return (
    <>
      {originalDriverData ? (
        <TableList
          columns={columns}
          data={originalDriverData}
          onEdit={handleEditClick}
          isLoading={isLoading}
          scrollbarMarginTop="48px"
        />
      ) : (
        <p
          style={{
            padding: ' 40px 50px 44px 50px',
            backgroundColor: 'white',
            borderRadius: 7,
            textAlign: 'center'
          }}
        >
          No JDBC Drivers data yet.
        </p>
      )}
      {isEditModalOpen && currentRow && row && (
        <EditTableModal
          title={`Edit ${row.databaseType} ${row.version}`}
          settings={currentRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </>
  )
}

export default ConfigJDBCDrivers
