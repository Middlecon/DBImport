import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Button from '../../components/Button'
import { useQueryClient } from '@tanstack/react-query'
import {
  Connections,
  ConnectionSearchFilter,
  EditSetting,
  UiConnectionSearchFilter
} from '../../utils/interfaces'
import { useAtom } from 'jotai'
import { connectionPersistStateAtom } from '../../atoms/atoms'
import ConnectionSearchFilterCns from './ConnectionSearchFilterCns'
import CreateConnectionModal from '../../components/modals/CreateConnectionModal'
import { createConnectionData } from '../../utils/dataFunctions'
import { useCreateOrUpdateConnection } from '../../utils/mutations'

interface ConnectionActionsProps {
  connections: Connections[] | undefined
  filters: ConnectionSearchFilter
}

function ConnectionActions({ connections, filters }: ConnectionActionsProps) {
  const { mutate: createConnection } = useCreateOrUpdateConnection()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [, setConnectionPersistState] = useAtom(connectionPersistStateAtom)

  const handleSave = (newConnectionSettings: EditSetting[]) => {
    const newConnectionData = createConnectionData(newConnectionSettings)
    console.log('newConnectionData', newConnectionData)

    createConnection(newConnectionData, {
      onSuccess: (response) => {
        // For getting fresh data from database to the cache
        queryClient.invalidateQueries({
          queryKey: ['connection', 'search', filters]
        })
        queryClient.invalidateQueries({
          queryKey: ['connection']
        })
        console.log('Update successful', response)
        setIsCreateModalOpen(false)
      },
      onError: (error) => {
        console.error('Error updating connection', error)
      }
    })
  }

  const handleShow = (uiFilters: UiConnectionSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiConnectionSearchFilter)[] = [
      'name',
      'connectionString',
      'serverType'
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
      setConnectionPersistState(`/connection?${orderedSearch}`)
      navigate(`/connection?${orderedSearch}`, { replace: true })
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
        <Button
          title="+ Create"
          onClick={() => setIsCreateModalOpen(true)}
          fontFamily={`'Work Sans Variable', sans-serif`}
          fontSize="14px"
        />

        <ConnectionSearchFilterCns
          isSearchFilterOpen={openDropdown === 'searchFilter'}
          onToggle={(isSearchFilterOpen: boolean) =>
            handleDropdownToggle('searchFilter', isSearchFilterOpen)
          }
          onShow={handleShow}
          disabled={!connections}
        />
      </div>
      {isCreateModalOpen && (
        <CreateConnectionModal
          isCreateModalOpen={isCreateModalOpen}
          onSave={handleSave}
          onClose={() => setIsCreateModalOpen(false)}
        />
      )}
    </>
  )
}

export default ConnectionActions
