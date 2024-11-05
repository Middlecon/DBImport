import { Outlet, useNavigate, useParams } from 'react-router-dom'
import Dropdown from '../../components/Dropdown'
import { useExportConnections, useExportTables } from '../../utils/queries'
import '../import/Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import { EditSetting } from '../../utils/interfaces'
// import { createTableData } from '../../utils/dataFunctions'
// import { useQueryClient } from '@tanstack/react-query'
import { useAtom } from 'jotai'
import {
  exportCnListFiltersAtom,
  isDbDropdownReadyAtom,
  selectedExportConnectionAtom
} from '../../atoms/atoms'
import CreateExportTableModal from '../../components/CreateExportTableModal'
import { createExportTableData } from '../../utils/dataFunctions'
import { useCreateExportTable } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
// import { useCreateExportTable } from '../../utils/mutations'

function Export() {
  const { data, isLoading } = useExportConnections()

  const cnNames = useMemo(() => {
    return Array.isArray(data) ? data.map((connection) => connection.name) : []
  }, [data])

  const navigate = useNavigate()
  const { connection } = useParams<{ connection: string }>()
  const { data: tables } = useExportTables(connection ? connection : null)
  const { mutate: createTable } = useCreateExportTable()
  const queryClient = useQueryClient()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)
  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )
  const [selectedExportConnection, setSelectedExportConnection] = useAtom(
    selectedExportConnectionAtom
  )
  const [, setSelectedFilters] = useAtom(exportCnListFiltersAtom)

  useEffect(() => {
    if (isLoading || !cnNames.length) return

    if (connection && cnNames.includes(connection)) {
      if (connection !== selectedExportConnection) {
        setSelectedExportConnection(connection)
      }
    } else {
      setSelectedExportConnection(null)
      setSelectedFilters({})

      navigate('/export', { replace: true })
    }

    setIsDbDropdownReady(true)
  }, [
    cnNames,
    connection,
    isLoading,
    navigate,
    selectedExportConnection,
    setIsDbDropdownReady,
    setSelectedExportConnection,
    setSelectedFilters
  ])

  const handleSelect = (item: string | null) => {
    setSelectedFilters({})
    navigate(`/export/${item}`)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const mostCommonConnection = useMemo(() => {
    if (!tables || tables.length < 1) return null
    const connectionCounts = tables.reduce((acc, row) => {
      const connection = row.connection
      if (connection) {
        acc[connection] = (acc[connection] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    return Object.keys(connectionCounts).reduce((a, b) =>
      connectionCounts[a] > connectionCounts[b] ? a : b
    )
  }, [tables])

  const handleSave = (newTableData: EditSetting[]) => {
    console.log('newTableData', newTableData)
    const newTable = createExportTableData(newTableData)
    console.log('newTable', newTable)

    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['export', newTable.connection]
        })
        console.log('Create successful', response)
        setCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error creating table', error)
      }
    })
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="import-header">
          <h1>Export</h1>
          <div className="db-dropdown">
            {selectedExportConnection && (
              <Button
                title="+ Create table"
                onClick={() => setCreateModalOpen(true)}
                fontFamily={`'Work Sans Variable', sans-serif`}
                fontSize="14px"
                padding="4px 13px 7.5px 9px"
              />
            )}
            {isDbDropdownReady && (
              <Dropdown
                items={cnNames.length > 0 ? cnNames : ['No Connection yet']}
                onSelect={handleSelect}
                isOpen={openDropdown === 'cnSearch'}
                onToggle={(isOpen: boolean) =>
                  handleDropdownToggle('cnSearch', isOpen)
                }
                searchFilter={true}
                initialTitle={selectedExportConnection || 'Select Connection'}
                leftwards={true}
                chevron={true}
                placeholder="Search for db..."
              />
            )}
          </div>
        </div>

        {!connection ? (
          <div className="import-text-block">
            <p>
              Please select a DB in the above dropdown to show and edit settings
              for its tables.
            </p>
          </div>
        ) : (
          <>
            <Outlet />
            {isCreateModalOpen && mostCommonConnection && (
              <CreateExportTableModal
                connection={connection}
                onSave={handleSave}
                onClose={() => setCreateModalOpen(false)}
              />
            )}
          </>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Export
