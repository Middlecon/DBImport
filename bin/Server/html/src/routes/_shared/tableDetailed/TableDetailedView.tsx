import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../tableDetailed/DetailedView.scss'
import DropdownActions from '../../../components/DropdownActions'
import RepairTableModal from '../../../components/modals/RepairTableModal'
import RepairIcon from '../../../assets/icons/RepairIcon'
import ResetIcon from '../../../assets/icons/ResetIcon'
import ResetTableModal from '../../../components/modals/ResetTableModal'
import CopyTableModal from '../../../components/modals/CopyTableModal'
import {
  newCopyExportTableData,
  newCopyImportTableData
} from '../../../utils/dataFunctions'
import {
  EditSetting,
  UIExportTableWithoutEnum,
  UITableWithoutEnum
} from '../../../utils/interfaces'
import { useCopyTable } from '../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useRawTable } from '../../../utils/queries'
import CopyIcon from '../../../assets/icons/CopyIcon'

interface TableDetailedViewProps {
  type: 'import' | 'export'
}

function TableDetailedView({ type }: TableDetailedViewProps) {
  const { database, table, connection, targetSchema, targetTable } = useParams()

  const primaryKeys =
    type === 'import'
      ? database && table
        ? { database, table }
        : null
      : connection && targetSchema && targetTable
      ? { connection, targetSchema, targetTable }
      : null

  const { data: tableData } = useRawTable({
    type,
    databaseOrConnection: type === 'import' ? database : connection,
    schema: type === 'export' ? targetSchema : undefined,
    table: type === 'import' ? table : targetTable
  })

  const exportDatabase = type === 'export' ? tableData?.database : null

  const queryClient = useQueryClient()
  const { mutate: copyTable } = useCopyTable()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isRepairTableModalOpen, setIsRepairTableModalOpen] = useState(false)
  const [isResetTableModalOpen, setIsResetTableModalOpen] = useState(false)
  const [isCopyTableModalOpen, setIsCopyTableModalOpen] = useState(false)

  const encodedDatabase = encodeURIComponent(database ? database : '')
  const encodedTable = encodeURIComponent(table ? table : '')
  const encodedConnection = encodeURIComponent(connection ? connection : '')
  const encodedTargetSchema = encodeURIComponent(
    targetSchema ? targetSchema : ''
  )
  const encodedTargetTable = encodeURIComponent(targetTable ? targetTable : '')

  const navigate = useNavigate()
  const location = useLocation()

  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab =
    type === 'export'
      ? pathSegments[4] || 'settings'
      : pathSegments[3] || 'settings'

  const validTabs = useMemo(() => ['settings', 'columns', 'statistics'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      if (type === 'import') {
        navigate(`/import/${encodedDatabase}/${encodedTable}/settings`, {
          replace: true
        })
      } else if (type === 'export') {
        navigate(
          `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/settings`,
          {
            replace: true
          }
        )
      }
    }
  }, [
    tab,
    database,
    table,
    navigate,
    validTabs,
    type,
    connection,
    encodedDatabase,
    encodedTable,
    encodedConnection,
    encodedTargetSchema,
    encodedTargetTable
  ])

  const handleTabClick = (tabName: string) => {
    if (type === 'import') {
      navigate(`/import/${encodedDatabase}/${encodedTable}/${tabName}`)
    } else if (type === 'export') {
      navigate(
        `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/${tabName}`
      )
    }
  }

  const handleRepairTableClick = () => {
    setIsRepairTableModalOpen(true)
  }

  const handleResetTableClick = () => {
    setIsResetTableModalOpen(true)
  }

  const handleCopyIconClick = () => {
    setIsCopyTableModalOpen(true)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleCopySave = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return
    console.log('tablePrimaryKeysSettings', tablePrimaryKeysSettings)
    console.log('tableData', tableData)

    // Dynamically determine the new table data based on type
    const newCopyTableData =
      type === 'import'
        ? newCopyImportTableData(
            tableData as UITableWithoutEnum,
            tablePrimaryKeysSettings
          )
        : newCopyExportTableData(
            tableData as UIExportTableWithoutEnum,
            tablePrimaryKeysSettings
          )

    console.log(newCopyTableData)

    // Set the appropriate query keys and handle copy logic
    copyTable(
      { type, table: newCopyTableData },
      {
        onSuccess: (response) => {
          queryClient.invalidateQueries({
            queryKey: [type, 'search'], // Dynamic queryKey for import/export
            exact: false
          })

          if (type === 'import') {
            queryClient.invalidateQueries({
              queryKey: ['databases'], // Additional invalidation for import
              exact: false
            })
          }

          console.log('Save table copy successful', response)
          setIsCopyTableModalOpen(false)
        },
        onError: (error) => {
          console.error('Error copy table', error)
        }
      }
    )
  }

  const dropdownActionsItems = useMemo(() => {
    if (!tableData) return []

    const items = [
      {
        icon: <CopyIcon />,
        label: `Copy table`,
        onClick: handleCopyIconClick
      }
    ]

    if (
      (type === 'import' && tableData.importPhaseType === 'incr') ||
      (type === 'export' && tableData.exportType === 'incr')
    ) {
      items.unshift(
        {
          icon: <RepairIcon />,
          label: `Repair table`,
          onClick: handleRepairTableClick
        },
        {
          icon: <ResetIcon />,
          label: `Reset table`,
          onClick: handleResetTableClick
        }
      )
    }

    return items
  }, [tableData, type])

  return (
    <>
      <ViewBaseLayout>
        <div className="detailed-view-header">
          {type === 'import' && <h1>{`${database}.${table}`}</h1>}
          {type === 'export' && (
            <h1>{`${connection}.${targetSchema}.${targetTable}`}</h1>
          )}
          <DropdownActions
            isDropdownActionsOpen={openDropdown === 'dropdownActions'}
            marginTop={5}
            onToggle={(isDropdownActionsOpen: boolean) =>
              handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
            }
            items={dropdownActionsItems}
          />
        </div>
        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'columns' ? 'active-tab' : ''}
            onClick={() => handleTabClick('columns')}
          >
            Columns
          </h2>
          <h2
            style={{ color: '#a4a4a4', cursor: 'default' }}
            // className={selectedTab === 'statistics' ? 'active-tab' : ''}
            // onClick={() => handleTabClick('statistics')}
          >
            Statistics
          </h2>
        </div>
        <Outlet />
        {isRepairTableModalOpen && primaryKeys && (
          <RepairTableModal
            type={type}
            primaryKeys={primaryKeys}
            isRepairTableModalOpen={isRepairTableModalOpen}
            onClose={() => setIsRepairTableModalOpen(false)}
            exportDatabase={exportDatabase ? exportDatabase : ''}
          />
        )}
        {isResetTableModalOpen && primaryKeys && (
          <ResetTableModal
            type={type}
            primaryKeys={primaryKeys}
            isResetTableModalOpen={isResetTableModalOpen}
            onClose={() => setIsResetTableModalOpen(false)}
            exportDatabase={exportDatabase ? exportDatabase : ''}
          />
        )}
        {isCopyTableModalOpen && primaryKeys && tableData && (
          <CopyTableModal
            type={type}
            primaryKeys={primaryKeys}
            isCopyTableModalOpen={isCopyTableModalOpen}
            onSave={handleCopySave}
            onClose={() => setIsCopyTableModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
