import { useDbTables } from '../../utils/queries'
import './DbTables.scss'

function DbTable() {
  const { data, isLoading, isError, error } = useDbTables()

  if (isLoading) {
    return <p>Loading...</p>
  }

  if (isError) {
    return <p>Error: {error?.message}</p>
  }

  console.log('db tables', data)

  return (
    <div className="db-table">
      <p>DB table</p>
      <ul>
        {data?.map((table) => (
          <li key="table.table">{table.table}</li>
        ))}
      </ul>
    </div>
  )
}

export default DbTable
