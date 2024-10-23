import { useParams } from 'react-router-dom'
import { useAirflowDAG } from '../../utils/queries'
import '../import/tableDetailed/TableDetailedView.scss'
import CardAirflow from './CardAirflow'
import { airflowCardRenderFormatting } from '../../utils/cardRenderFormatting'

function AirflowSettings({ type }: { type: 'import' | 'export' | 'custom' }) {
  const { dagName } = useParams<{
    dagName: string
  }>()
  const { data: dagData, isFetching } = useAirflowDAG(type, dagName)

  if (isFetching) return <div>Loading...</div>
  if (!dagName) return <div>No data found.</div>
  console.log('dagName', dagName)

  if (!dagData) {
    console.error('Table data is not available.')
    return
  }
  console.log('dagData', dagData)
  const airflowImportDagSettings = airflowCardRenderFormatting(type, dagData)

  if (!airflowImportDagSettings) {
    console.error('Table data is not available.')
    return
  }

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div>
            <div className="cards-container">
              <CardAirflow
                title="Settings"
                settings={airflowImportDagSettings}
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
