import { useParams } from 'react-router-dom'
import { UITable } from '../../../../utils/interfaces'
import './TableSettings.scss'
import CardsRenderer from './CardsRenderer'
import { useQueryClient } from '@tanstack/react-query'

function TableSettings() {
  const { table } = useParams<{ table: string }>()

  const queryClient = useQueryClient()
  const tableData: UITable | undefined = queryClient.getQueryData([
    'table',
    table
  ])

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          {tableData && <CardsRenderer table={tableData} />}
        </div>
      </div>
    </>
  )
}

export default TableSettings
