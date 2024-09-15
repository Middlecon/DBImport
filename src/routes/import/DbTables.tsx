import DropdownCheckbox from '../../components/DropdownCheckbox'
import DropdownRadio from '../../components/DropdownRadio'
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
  { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
  { header: 'Actions', isAction: true }
]

const checkboxFilters = [
  {
    title: 'Import Type',
    values: ['Full', 'Incremental', 'Oracle Flashback', 'MSSQL Change Tracking']
  },
  {
    title: 'ETL Type',
    values: [
      'Truncate and Insert',
      'Insert only',
      'Merge',
      'Merge with History Audit',
      'Only create external table',
      'None'
    ]
  },
  {
    title: 'Import Tool',
    values: ['Spark', 'Sqoop']
  },
  {
    title: 'ETL Engine',
    values: ['Hive', 'Spark']
  }
]

const radioFilters = [
  {
    title: 'Last update from source',
    radioName: 'timestamp',
    badgeContent: ['D', 'W', 'M', 'Y'],
    values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  },
  {
    title: 'Include in Airflow',
    radioName: 'includeInAirflow',
    badgeContent: ['y', 'n'],
    values: ['Yes', 'No']
  }
]

function DbTable() {
  const { data, isLoading, isError, error } = useDbTables()
  console.log('data dbTables', data)

  if (isLoading) {
    return <p>Loading...</p>
  }

  if (isError) {
    return <p>Error: {error?.message}</p>
  }

  const handleSelect = (items: string[]) => {
    console.log('items', items)
  }

  return (
    <div className="db-table-root">
      <div className="filters">
        {checkboxFilters.map((filter, index) => (
          <DropdownCheckbox
            key={index}
            items={filter.values}
            title={filter.title}
            onSelect={handleSelect}
          />
        ))}
        {radioFilters.map((filter, index) => (
          <DropdownRadio
            key={index}
            items={filter.values}
            title={filter.title}
            radioName={filter.radioName}
            badgeContent={filter.badgeContent}
            onSelect={handleSelect}
          />
        ))}
      </div>
      {data ? (
        <TableList columns={columns} data={data} />
      ) : (
        <div>Loading....</div>
      )}
    </div>
  )
}

export default DbTable
