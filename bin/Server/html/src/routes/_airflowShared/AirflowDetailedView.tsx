import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../_shared/tableDetailed/DetailedView.scss'
import GenerateDAGIcon from '../../assets/icons/GenerateDAGIcon'
import DropdownActions from '../../components/DropdownActions'
import GenerateDagModal from '../../components/modals/GenerateDagModal'
import UrlLinkIcon from '../../assets/icons/UrlLinkIcon'
import { useAirflowDAG } from '../../utils/queries'

function AirflowDetailedView({
  type
}: {
  type: 'import' | 'export' | 'custom'
}) {
  const { dagName } = useParams()
  const navigate = useNavigate()
  const location = useLocation()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isGenDagModalOpen, setIsGenDagModalOpen] = useState(false)

  const encodedType = encodeURIComponent(type)
  const encodedDagName = encodeURIComponent(dagName ? dagName : '')

  // const pathSegments = location.pathname.split('/').filter(Boolean)
  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab = pathSegments[3] || 'settings'
  const validTabs = useMemo(() => ['settings', 'tasks'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      navigate(`/airflow/${encodedType}/${encodedDagName}/settings`, {
        replace: true
      })
    }
  }, [tab, navigate, validTabs, dagName, type, encodedType, encodedDagName])

  const { data: dagData, isError } = useAirflowDAG(type, dagName)
  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (!dagName && !dagData && !isError)
    return <div className="loading">Loading...</div>

  if (!dagData) {
    return
  }

  const handleTabClick = (tabName: string) => {
    navigate(`/airflow/${encodedType}/${encodedDagName}/${tabName}`)
  }

  const handleGenerateDagClick = () => {
    setIsGenDagModalOpen(true)
  }

  const handleLinkClick = () => {
    console.log('item', dagData.airflowLink)
    if (dagData.airflowLink) {
      window.open(dagData.airflowLink, '_blank')
    } else {
      console.error('No Airlow link on DAG.')
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
      <ViewBaseLayout>
        <div className="detailed-view-header">
          <h1>{`${dagName}`}</h1>
          <div className="actions-dropdown-generated-br">
            <DropdownActions
              isDropdownActionsOpen={openDropdown === 'dropdownActions'}
              marginTop={5}
              maxWidth={107}
              onToggle={(isDropdownActionsOpen: boolean) =>
                handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
              }
              items={[
                {
                  icon: <GenerateDAGIcon />,
                  label: 'Generate DAG',
                  onClick: handleGenerateDagClick
                },
                {
                  icon: <UrlLinkIcon />,
                  label: 'Airflow DAG external link',
                  onClick: handleLinkClick
                }
              ]}
            />
          </div>
        </div>

        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'tasks' ? 'active-tab' : ''}
            onClick={() => handleTabClick('tasks')}
          >
            Tasks
          </h2>
        </div>
        <Outlet />
        {isGenDagModalOpen && dagName && (
          <GenerateDagModal
            dagName={dagName}
            isGenDagModalOpen={isGenDagModalOpen}
            onClose={() => setIsGenDagModalOpen(false)}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowDetailedView
