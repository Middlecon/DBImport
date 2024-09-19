import './TableList.scss'
import { Column, UiDbTable } from '../utils/interfaces'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

interface TableProps {
  columns: Column[]
  data: UiDbTable[]
}

function TableList({ columns, data }: TableProps) {
  const [overflowState, setOverflowState] = useState<boolean[]>([])
  const navigate = useNavigate()

  const cellRefs = useRef<(HTMLParagraphElement | null)[]>([])
  // console.log('data TableList', data)

  const handleTableClick = (db: string, table: string) => {
    navigate(`/import/${db}/${table}/settings`)
  }

  useEffect(() => {
    const isOverflowing = cellRefs.current.map((el) =>
      el ? el.scrollWidth > el.clientWidth : false
    )
    setOverflowState(isOverflowing)
  }, [data, columns])
  return (
    <table className="custom-table-root">
      <thead>
        <tr>
          {columns.map((column, index) => (
            <th
              key={index}
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
                      {row[column.accessor as keyof UiDbTable]}
                    </p>
                    {overflowState[rowIndex] && (
                      <span className="tooltip">
                        {row[column.accessor as keyof UiDbTable]}
                      </span>
                    )}
                  </>
                ) : column.accessor === 'table' ? (
                  <>
                    <p
                      ref={(el) => (cellRefs.current[rowIndex] = el)}
                      onClick={() => handleTableClick(row.database, row.table)}
                      className="clickable-table-name"
                    >
                      {row[column.accessor as keyof UiDbTable]}
                    </p>
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
                  (row[
                    `${column.accessor}Display` as keyof UiDbTable
                  ] as string) ?? row[column.accessor as keyof UiDbTable]
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
