import TableList from '../../components/TableList'
import { Column } from '../../utils/interfaces'
import { useDbTables } from '../../utils/queries'
import './DbTables.scss'

const columns: Column[] = [
  { header: 'Table', accessor: 'table' },
  { header: 'Connection', accessor: 'connection' },
  { header: 'Source Schema', accessor: 'sourceSchema' },
  { header: 'Source Table', accessor: 'sourceTable' },
  { header: 'Import Type', accessor: 'importPhaseType' },
  { header: 'ETL Type', accessor: 'etlPhaseType' },
  { header: 'Import Tool', accessor: 'importTool' },
  { header: 'ETL Engine', accessor: 'etlEngine' },
  { header: 'Timestamp', accessor: 'lastUpdateFromSource' },
  { header: 'Actions', isAction: true }
]

function DbTable() {
  const { data, isLoading, isError, error } = useDbTables()

  if (isLoading) {
    return <p>Loading...</p>
  }

  if (isError) {
    return <p>Error: {error?.message}</p>
  }

  return (
    <>
      <div className="db-table">
        {data ? (
          <TableList columns={columns} data={data} />
        ) : (
          <div>Loading....</div>
        )}
      </div>
    </>
  )
}

export default DbTable
