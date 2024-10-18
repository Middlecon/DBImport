import { useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../import/tableDetailed/TableDetailedView.scss'
import '../../import/tableDetailed/settings/CardsRenderer.scss'
import '../../import/tableDetailed/settings/TableSettings.scss'

import CardsRendererConnection from './CardsRendererConnection'

function ConnectionDetailedView() {
  const { connection } = useParams<{ connection: string }>()

  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>{`${connection}`}</h1>
        </div>
        <div className="block-container" style={{ margin: 0 }}>
          <div className="block-container-2">
            <CardsRendererConnection />
          </div>
        </div>
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
