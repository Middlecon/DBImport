import { useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../import/tableDetailed/TableDetailedView.scss'
import '../../import/tableDetailed/settings/TableSettings.scss'
import { useConnection } from '../../../utils/queries'
import { connectionCardRenderSettings } from '../../../utils/cardRenderFormatting'
import CardConnection from './CardConnection'
import '../../../components/Loading.scss'

function ConnectionDetailedView() {
  const { connection: connectionParam } = useParams<{
    connection: string
  }>()
  const { data: connection, isFetching } = useConnection(connectionParam)

  if (isFetching) return <div className="loading">Loading...</div>
  if (!connection) return <div>No data found.</div>
  console.log('connection', connection)

  const connectionSettings = connectionCardRenderSettings(connection)

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>{`${connectionParam}`}</h1>
        </div>
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
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
