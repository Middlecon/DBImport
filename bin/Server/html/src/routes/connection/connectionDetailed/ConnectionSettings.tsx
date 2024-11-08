import { useParams } from 'react-router-dom'
import '../../_shared/TableDetailedView.scss'
import '../../import/tableDetailed/settings/TableSettings.scss'
import { useConnection } from '../../../utils/queries'
import { connectionCardRenderSettings } from '../../../utils/cardRenderFormatting'
import CardConnection from './CardConnection'
import '../../../components/Loading.scss'

function ConnectionSettings() {
  const { connection: connectionParam } = useParams<{
    connection: string
  }>()
  const { data: connection, isError } = useConnection(connectionParam)

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }

  if (!connection && !isError) return <div className="loading">Loading...</div>

  const connectionSettings = connectionCardRenderSettings(connection)

  return (
    <div className="block-container" style={{ margin: 0 }}>
      <div className="block-container-2">
        <div>
          <div className="cards-container">
            <CardConnection
              title="Settings"
              settings={connectionSettings}
              originalData={connection}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export default ConnectionSettings
