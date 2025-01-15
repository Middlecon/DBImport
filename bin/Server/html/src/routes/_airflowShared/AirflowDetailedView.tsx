import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../_shared/TableDetailedView.scss'
import { AxiosError } from 'axios'
import GenerateDAGIcon from '../../assets/icons/GenerateDAGIcon'
import DropdownActions from '../../components/DropdownActions'
import { useGenDagConnection } from '../../utils/mutations'
import { ErrorData } from '../connection/connectionDetailed/ConnectionDetailedView'
import EditTableModal from '../../components/EditTableModal'
import { generateDagSettings } from '../../utils/cardRenderFormatting'

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
  const [isGenDagLoading, setIsGenDagLoading] = useState(false)
  const [successMessageGenerate, setSuccessMessageGenerate] = useState<
    string | null
  >(null)
  const [errorMessageGenerate, setErrorMessageGenerate] = useState<
    string | null
  >(null)
  const settings = generateDagSettings(dagName ? dagName : '')

  const { mutate: generateDag } = useGenDagConnection()

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

  const handleTabClick = (tabName: string) => {
    navigate(`/airflow/${encodedType}/${encodedDagName}/${tabName}`)
  }

  const handleGenerateDagClick = () => {
    setIsGenDagModalOpen(true)
  }

  const handleGenerateDag = () => {
    if (!dagName) {
      console.log('No dagName', dagName)
      return
    }

    setIsGenDagLoading(true)

    generateDag(dagName, {
      onSuccess: (response) => {
        setIsGenDagLoading(false)
        setSuccessMessageGenerate('Generate DAG succeeded')
        console.log('Generating DAG succeeded, result:', response)
        // setIsGenDagModalOpen(false)
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.statusText &&
          error.response?.data.detail !== undefined
            ? `${error.message} ${error.response?.statusText}, ${error.response?.data.detail[0].msg}: ${error.response?.data.detail[0].type}`
            : error.status === 500
            ? `${error.message} ${error.response?.statusText}: ${error.response?.data}`
            : 'An unknown error occurred'
        setIsGenDagLoading(false)
        setErrorMessageGenerate(errorMessage)
        console.log('error', error)
        console.error('Generate DAG failed', error.message)
      }
    })
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
        <div className="table-header">
          <h1>{`${dagName}`}</h1>
          {/* <div style={{ marginTop: 5 }}> */}
          <DropdownActions
            isDropdownActionsOpen={openDropdown === 'dropdownActions'}
            marginTop={5}
            onToggle={(isDropdownActionsOpen: boolean) =>
              handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
            }
            items={[
              {
                icon: <GenerateDAGIcon />,
                label: `Generate DAG`,
                onClick: handleGenerateDagClick
              }
            ]}
          />
        </div>
        {/* </div> */}
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
        {isGenDagModalOpen && (
          <EditTableModal
            isEditModalOpen={isGenDagModalOpen}
            title={`Generate DAG`}
            settings={settings}
            onSave={handleGenerateDag}
            onClose={() => {
              setIsGenDagModalOpen(false)
              setErrorMessageGenerate(null)
              setSuccessMessageGenerate(null)
            }}
            isNoCloseOnSave={true}
            initWidth={300}
            isLoading={isGenDagLoading}
            loadingText="Generating"
            successMessage={successMessageGenerate}
            errorMessage={errorMessageGenerate ? errorMessageGenerate : null}
            onResetErrorMessage={() => setErrorMessageGenerate(null)}
            submitButtonTitle="Generate"
            closeButtonTitle="Close"
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowDetailedView
