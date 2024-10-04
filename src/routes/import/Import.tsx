import { Outlet, useNavigate, useParams } from 'react-router-dom'
import Dropdown from '../../components/Dropdown'
import { useDatabases } from '../../utils/queries'
import './Import.scss'
import { useEffect, useMemo, useState } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import Button from '../../components/Button'
import CreateTableModal from '../../components/CreateTableModal'
import { TableSetting } from '../../utils/interfaces'

function Import() {
  const { data, isLoading } = useDatabases()
  // console.log('data DATABASES', data)

  const databaseNames = useMemo(
    () => data?.map((database) => database.name) ?? [],
    [data]
  )

  const navigate = useNavigate()
  const { database } = useParams<{ database: string }>()
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null)
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isModalOpen, setModalOpen] = useState(false)

  useEffect(() => {
    if (isLoading) return
    if (database && databaseNames.includes(database)) {
      setSelectedDatabase(database)
    } else if (database) {
      navigate('/import', { replace: true })
    }
  }, [database, databaseNames, isLoading, navigate])

  const handleSelect = (item: string) => {
    navigate(`/import/${item}`)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const outletContext = {
    openDropdown,
    handleDropdownToggle
  }

  const handleSave = (newTableData: TableSetting[]) => {
    console.log('newTableData', newTableData)
    // if (!tableData) {
    //   console.error('Table data is not available.')
    //   return
    // }
    // console.log('tableData', tableData)

    // const editedTableData = updateTableData(tableData, updatedSettings)
    // updateTable(editedTableData, {
    //   onSuccess: (response) => {
    //     queryClient.invalidateQueries({
    //       queryKey: ['tables', tableData.database]
    //     })
    //     console.log('Update successful', response)
    //     setModalOpen(false)
    //   },
    //   onError: (error) => {
    //     console.error('Error updating table', error)
    //   }
    // })
  }

  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import']}>
        <div className="import-header">
          <h1>Import</h1>
          <div className="db-dropdown">
            {selectedDatabase && (
              <Button
                title="+ Create table"
                onClick={() => setModalOpen(true)}
                fontFamily={`'Work Sans Variable', sans-serif`}
                fontSize="14px"
                padding="4px 13px 7.5px 9px"
              />
            )}
            <Dropdown
              items={databaseNames.length > 0 ? databaseNames : ['No DB yet']}
              onSelect={handleSelect}
              isOpen={openDropdown === 'dbSearch'}
              onToggle={(isOpen: boolean) =>
                handleDropdownToggle('dbSearch', isOpen)
              }
              searchFilter={true}
              initialTitle={selectedDatabase || 'Select DB'}
              leftwards={true}
              chevron={true}
              placeholder="Search for db..."
            />
          </div>
        </div>

        {!database ? (
          <div className="import-text-block">
            <p>
              Please select a DB in the above dropdown to show and edit settings
              for its tables.
            </p>
          </div>
        ) : (
          <>
            <Outlet context={outletContext} />
            {isModalOpen && (
              <CreateTableModal
                database={database}
                onSave={handleSave}
                onClose={() => setModalOpen(false)}
              />
            )}
          </>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Import
