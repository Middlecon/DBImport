import './TableList.scss'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Column } from '../utils/interfaces'

interface TableProps<T> {
  columns: Column<T>[]
  data: T[]
  isLoading: boolean
  onEdit?: (row: T) => void
}
function TableList<T>({ columns, data, isLoading, onEdit }: TableProps<T>) {
  const [loading, setLoading] = useState(true)
  const [overflowState, setOverflowState] = useState<boolean[]>([])
  const navigate = useNavigate()
  const cellRefs = useRef<(HTMLParagraphElement | null)[]>([])

  const handleTableClick = (db: string, table: string) => {
    navigate(`/import/${db}/${table}/settings`)
  }

  // Sets loading state and checks if each table cell is overflowing to determine if a tooltip should be displayed for that cell
  useEffect(() => {
    if (isLoading) {
      setLoading(true)
    } else {
    const isOverflowing = cellRefs.current.map((el) =>
      el ? el.scrollWidth > el.clientWidth : false
    )
    setOverflowState(isOverflowing)
      setLoading(false)
    }
  }, [data, columns, isLoading])

  const renderCellContent = (row: T, column: Column<T>, rowIndex: number) => {
    const accessorKey = column.accessor as keyof T
    const displayKey = `${String(accessorKey)}Display` as keyof T

    const cellValue = row[displayKey] ?? row[accessorKey] ?? ''

    if (column.accessor === 'sourceTable') {
      return (
        <>
          <p ref={(el) => (cellRefs.current[rowIndex] = el)}>
            {String(cellValue)}
          </p>
          {overflowState[rowIndex] && (
            <span className="tooltip">{String(cellValue)}</span>
          )}
        </>
      )
    }

    if (column.accessor === 'table') {
      return (
        <p
          ref={(el) => (cellRefs.current[rowIndex] = el)}
          onClick={() =>
            handleTableClick(
              String(row['database' as keyof T]),
              String(row['table' as keyof T])
            )
          }
          className="clickable-table-name"
        >
          {String(cellValue)}
        </p>
      )
    }

    if (column.isAction) {
      return (
        <div className="actions-row">
          {column.isAction === 'edit' || column.isAction === 'both' ? (
            <button onClick={() => onEdit && onEdit(row)} disabled={!onEdit}>
              <EditIcon />
            </button>
          ) : null}
          {column.isAction === 'delete' || column.isAction === 'both' ? (
            <button
              className="actions-delete-button"
              onClick={() => console.log('Delete', row)}
            >
              <DeleteIcon />
            </button>
          ) : null}
        </div>
      )
    }

    return String(cellValue)
  }

  return (
    <div className="tables-container">
      {loading ? (
        <div className="loading-container">
          <p>Loading tables...</p>
        </div>
      ) : (
        <div
          className="scrollable-container"
        >
        <table className="custom-table-root">
          <thead>
            <tr>
              {columns.map((column, index) => (
                <th
                  key={index}
                  className={
                    column.accessor === 'sourceTable' ? 'fixed-width' : ''
                  }
                >
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
              {data.length > 0 &&
                data.map((row, rowIndex) => (
              <tr key={rowIndex} className="dbtables-row">
                {columns.map((column) => (
                  <td
                    key={`${rowIndex}-${String(column.accessor)}`}
                    className={
                      column.accessor === 'sourceTable' ? 'fixed-width' : ''
                    }
                  >
                    {renderCellContent(row, column, rowIndex)}
                  </td>
                ))}
              </tr>
            ))}
              {data.length === 0 && !isLoading && (
                <tr>
                  <td colSpan={columns.length}>
                    <p
                      style={{
                        padding: ' 40px 50px 44px 50px',
                        backgroundColor: 'white',
                        borderRadius: 7,
                        textAlign: 'center'
                      }}
                    >
                      No data available
                    </p>
                  </td>
                </tr>
              )}
          </tbody>
        </table>
      </div>
      )}
    </div>
  )
}

export default TableList
