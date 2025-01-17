import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../tableDetailed/DetailedView.scss'
import DropdownActions from '../../../components/DropdownActions'
import RepairTableModal from '../../../components/modals/RepairTableModal'
import RepairIcon from '../../../assets/icons/RepairIcon'
import ResetIcon from '../../../assets/icons/ResetIcon'
import ResetTableModal from '../../../components/modals/ResetTableModal'

interface TableDetailedViewProps {
  type: 'import' | 'export'
}

function TableDetailedView({ type }: TableDetailedViewProps) {
  const { database, table, connection, targetSchema, targetTable } = useParams()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isRepairTableModalOpen, setIsRepairTableModalOpen] = useState(false)
  const [isResetTableModalOpen, setIsResetTableModalOpen] = useState(false)

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

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const primaryKeys =
    type === 'import'
      ? database && table
        ? { database, table }
        : null
      : connection && targetSchema && targetTable
      ? { connection, targetSchema, targetTable }
      : null

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
            items={[
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
            ]}
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
          />
        )}
        {isResetTableModalOpen && primaryKeys && (
          <ResetTableModal
            type={type}
            primaryKeys={primaryKeys}
            isResetTableModalOpen={isResetTableModalOpen}
            onClose={() => setIsResetTableModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
