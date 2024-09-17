import { useParams } from 'react-router-dom'
import './TableDetailedView.scss'

function TableDetailedView() {
  const { table } = useParams()
  return <div>Table Details for: {table}</div>
}

export default TableDetailedView
