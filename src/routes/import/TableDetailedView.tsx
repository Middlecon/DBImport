import { useParams } from 'react-router-dom'
import './TableDetailedView.scss'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useTable } from '../../utils/queries'

function TableDetailedView() {
  const { database, table } = useParams()
  const { data } = useTable()
  console.log('data TABLE DETAILED', data)
  return (
    <>
      <ViewBaseLayout breadcrumbs={['Import', `${database}`, `${table}`]}>
        <div className="import-header">
          <h1>{table}</h1>
        </div>
        <div>Table Details for: {table}</div>

        {/* {data && <div>{data}</div>} */}
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
