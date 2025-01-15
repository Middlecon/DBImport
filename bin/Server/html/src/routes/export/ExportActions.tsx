import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import ExportSearchFilterTables from '../../components/ExportSearchFilterTables'
import CreateExportTableModal from '../../components/CreateExportTableModal'
import { useQueryClient } from '@tanstack/react-query'
import { createExportTableData } from '../../utils/dataFunctions'
import {
  EditSetting,
  ExportSearchFilter,
  UIExportCnTables,
  UiExportSearchFilter
} from '../../utils/interfaces'
import { useCreateExportTable } from '../../utils/mutations'
import { useAtom } from 'jotai'
import {
  clearRowSelectionAtom,
  exportPersistStateAtom
} from '../../atoms/atoms'
import DiscoverIcon from '../../assets/icons/DiscoverIcon'
import PlusIcon from '../../assets/icons/PlusIcon'
import DiscoverImportModal from '../../components/DiscoverImportModal'
import DropdownActions from '../../components/DropdownActions'

interface ExportActionsProps {
  tables: UIExportCnTables[] | undefined
  filters: ExportSearchFilter
}

function ExportActions({ tables, filters }: ExportActionsProps) {
  const { mutate: createTable } = useCreateExportTable()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [, setExportPersistState] = useAtom(exportPersistStateAtom)
  const [, setClearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [isDiscoverModalOpen, setIsDiscoverModalOpen] = useState(false)

  const mostCommonConnection = useMemo(() => {
    if (!tables || tables.length === 0) return null
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
    const newTable = createExportTableData(newTableData)
    createTable(newTable, {
      onSuccess: (response) => {
        queryClient.invalidateQueries({
          queryKey: ['export', 'search', filters]
        })
        queryClient.invalidateQueries({
          queryKey: ['export', 'connections']
        })
        console.log('Create successful', response)
        setIsCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error creating table', error)
      }
    })
  }

  const handleShow = (uiFilters: UiExportSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiExportSearchFilter)[] = [
      'connection',
      'targetTable',
      'targetSchema',
      'includeInAirflow',
      'lastUpdateFromHive',
      'exportType',
      'exportTool'
    ]

    filterKeys.forEach((key) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(key, String(value))
      } else {
        params.delete(key)
      }
    })

    const orderedSearch = filterKeys
      .map((key) =>
        params.has(key) ? `${key}=${params.get(key) || ''}` : null
      )
      .filter((param) => param !== null)
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setExportPersistState(`/export?${orderedSearch}`)
      navigate(`/export?${orderedSearch}`, { replace: true })
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  return (
    <>
      <div className="header-buttons">
        <div className="actions-dropdown-discover-br">
          <DropdownActions
            isDropdownActionsOpen={openDropdown === 'dropdownActions'}
            onToggle={(isDropdownActionsOpen: boolean) =>
              handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
            }
            items={[
              {
                icon: <PlusIcon />,
                label: 'Create table',
                onClick: () => {
                  setClearRowSelectionTrigger((prev) => prev + 1)
                  setIsCreateModalOpen(true)
                  setOpenDropdown(null)
                }
              },
              {
                icon: <DiscoverIcon />,
                label: `Discover and Add tables`,
                onClick: () => {
                  setClearRowSelectionTrigger((prev) => prev + 1)
                  setIsDiscoverModalOpen(true)
                  setOpenDropdown(null)
                }
              }
            ]}
            disabled={!tables}
          />
        </div>

        <ExportSearchFilterTables
          isSearchFilterOpen={openDropdown === 'searchFilter'}
          onToggle={(isSearchFilterOpen: boolean) =>
            handleDropdownToggle('searchFilter', isSearchFilterOpen)
          }
          onShow={handleShow}
          disabled={!tables}
        />
      </div>
      {isCreateModalOpen && (
        <CreateExportTableModal
          isCreateModalOpen={isCreateModalOpen}
          prefilledConnection={
            mostCommonConnection ? mostCommonConnection : null
          }
          onSave={handleSave}
          onClose={() => setIsCreateModalOpen(false)}
        />
      )}
      {isDiscoverModalOpen && (
        <DiscoverImportModal
          title="Discover and Add tables"
          isDiscoverModalOpen={isDiscoverModalOpen}
          onClose={() => setIsDiscoverModalOpen(false)}
        />
      )}
    </>
  )
}

export default ExportActions
