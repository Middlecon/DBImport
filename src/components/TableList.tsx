import './TableList.scss'
import { Column, Table } from '../utils/interfaces'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import { useEffect, useRef, useState } from 'react'

interface TableProps {
  columns: Column[]
  data: Table[]
}

function TableList({ columns, data }: TableProps) {
  const [overflowState, setOverflowState] = useState<boolean[]>([])

  const cellRefs = useRef<(HTMLParagraphElement | null)[]>([])

  useEffect(() => {
    const isOverflowing = cellRefs.current.map((cell) =>
      cell ? cell.scrollWidth > cell.clientWidth : false
    )
    setOverflowState(isOverflowing)
  }, [data, columns])
  return (
    <table className="custom-table">
      <thead>
        <tr>
          {columns.map((column) => (
            <th
              key={column.header}
              className={column.accessor === 'sourceTable' ? 'fixed-width' : ''}
            >
              {column.header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, rowIndex) => (
          <tr key={rowIndex}>
            {columns.map((column) => (
              <td
                key={`${rowIndex}-${column.accessor}`}
                className={
                  column.accessor === 'sourceTable' ? 'fixed-width' : ''
                }
              >
                {column.accessor === 'sourceTable' ? (
                  <>
                    <p ref={(el) => (cellRefs.current[rowIndex] = el)}>
                      {row[column.accessor as keyof Table]}
                    </p>
                    {overflowState[rowIndex] && (
                      <span className="tooltip">
                        {row[column.accessor as keyof Table]}
                      </span>
                    )}
                  </>
                ) : column.isAction ? (
                  <div className="actions-row">
                    <button
                      className="actions-edit-button"
                      onClick={() => console.log('Edit', row)}
                    >
                      <EditIcon />
                    </button>
                    <button onClick={() => console.log('Delete', row)}>
                      <DeleteIcon />
                    </button>
                  </div>
                ) : (
                  <>{row[column.accessor as keyof Table]}</>
                )}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default TableList
