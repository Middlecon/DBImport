import { HeadersRowInfo } from '../utils/interfaces'
import './ListRowsInfo.scss'

interface ListRowsInfoProps<T> {
  filteredData: T[]
  headersRowInfo: HeadersRowInfo
  itemType: 'table' | 'connection'
}

function ListRowsInfo<T>({
  filteredData,
  headersRowInfo,
  itemType
}: ListRowsInfoProps<T>) {
  const totalRows = Number(headersRowInfo.contentTotalRows)
  const maxRows = Number(headersRowInfo.contentMaxReturnedRows)
  const itemTypeString = filteredData.length === 1 ? itemType : `${itemType}s`

  return (
    <p className="list-rows-info">
      {`Showing ${filteredData.length} ${
        totalRows > maxRows
          ? `(limited to ${headersRowInfo.contentMaxReturnedRows})`
          : ''
      } of ${
        headersRowInfo.contentTotalRows
      } ${itemTypeString} matching the filter`}
    </p>
  )
}

export default ListRowsInfo
