import { useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../_shared/TableDetailedView.scss'
import '../../import/tableDetailed/settings/TableSettings.scss'
import '../../../components/Loading.scss'
import ConnectionSettings from './ConnectionSettings'

function ConnectionDetailedView() {
  const { connection: connectionParam } = useParams<{
    connection: string
  }>()

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>{`${connectionParam}`}</h1>
        </div>
        <ConnectionSettings />
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
