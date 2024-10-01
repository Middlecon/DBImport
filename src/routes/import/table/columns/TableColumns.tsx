import { useParams } from 'react-router-dom'
import { Column, Columns, UITable } from '../../../../utils/interfaces'
import TableList from '../../../../components/TableList'
import { useQueryClient } from '@tanstack/react-query'

function TableColumns() {
  const { table } = useParams<{ table: string }>()

  const queryClient = useQueryClient()
  const tableData: UITable | undefined = queryClient.getQueryData([
    'table',
    table
  ])
  if (!tableData) return <span>Loading...</span>

  const columnsData = tableData.columns

  const columns: Column<Columns>[] = [
    { header: 'Column Name', accessor: 'columnName' },
    { header: 'Column Order', accessor: 'columnOrder' },
    { header: 'Source Column Name', accessor: 'sourceColumnName' },
    { header: 'Column Type', accessor: 'columnType' },
    { header: 'Source Column Type', accessor: 'sourceColumnType' },
    { header: 'Source Database Type', accessor: 'sourceDatabaseType' },
    { header: 'Column Name Override', accessor: 'columnNameOverride' },
    { header: 'Column Type Override', accessor: 'columnTypeOverride' },
    { header: 'Sqoop Column Type', accessor: 'sqoopColumnType' },
    {
      header: 'Sqoop Column Type Override',
      accessor: 'sqoopColumnTypeOverride'
    },
    { header: 'Force String', accessor: 'forceString' },
    { header: 'Include In Import', accessor: 'includeInImport' },
    { header: 'Source Primary Key', accessor: 'sourcePrimaryKey' },
    { header: 'Last Update From Source', accessor: 'lastUpdateFromSource' },
    { header: 'Comment', accessor: 'comment' },
    { header: 'Operator Notes', accessor: 'operatorNotes' },
    { header: 'Anonymization Function', accessor: 'anonymizationFunction' },
    { header: 'Edit', isAction: 'edit' }
  ]

  return (
    <div style={{ marginTop: 40 }}>
      <TableList columns={columns} data={columnsData} />
    </div>
  )
}

export default TableColumns
