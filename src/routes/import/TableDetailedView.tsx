import { useParams } from 'react-router-dom'
import './TableDetailedView.scss'
import ViewBaseLayout from '../../components/ViewBaseLayout'

function TableDetailedView() {
  const { db, table } = useParams()
  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import', `${db}`, `${table}`]}>
        <div className="import-header">
          <h1>{table}</h1>
        </div>
        <div>Table Details for: {table}</div>
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
