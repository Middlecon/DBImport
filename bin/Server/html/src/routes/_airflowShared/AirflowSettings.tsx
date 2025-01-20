import { useParams } from 'react-router-dom'
import { useAirflowDAG } from '../../utils/queries'
import '../_shared/tableDetailed/DetailedView.scss'
import CardAirflow from './CardAirflow'
import { airflowCardRenderSettings } from '../../utils/cardRenderFormatting'
import '../../components/Loading.scss'

function AirflowSettings({ type }: { type: 'import' | 'export' | 'custom' }) {
  const { dagName } = useParams<{
    dagName: string
  }>()
  const { data: dagData, isError } = useAirflowDAG(type, dagName)

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (!dagName && !dagData && !isError)
    return <div className="loading">Loading...</div>

  if (!dagData) {
    return
  }

  const airflowDagSettings = airflowCardRenderSettings(type, dagData)

  if (!airflowDagSettings) {
    console.error('DAG data is not available.')
    return
  }

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div>
            <div className="cards-container">
              <CardAirflow
                type={type}
                title="Settings"
                settings={airflowDagSettings}
                originalData={dagData}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default AirflowSettings
