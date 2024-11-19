import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import { Column } from '../utils/interfaces'
import './TableList.scss'

interface TableProps<T> {
  columns: Column<T>[]
  data: T[]
  isLoading: boolean
  onEdit?: (row: T, rowIndex?: number) => void
  scrollbarMarginTop?: string
  airflowType?: string
  isExport?: boolean
}

function TableList<T>({
  columns,
  data,
  isLoading,
  onEdit,
  scrollbarMarginTop,
  airflowType,
  isExport = false
}: TableProps<T>) {
  const [selectedRowIndex, setSelectedRowIndex] = useState<number | null>(null)
  const [visibleData, setVisibleData] = useState<T[]>([])
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [allDataLoaded, setAllDataLoaded] = useState(false)
  const [loading, setLoading] = useState(false)

  const [overflowState, setOverflowState] = useState<boolean[]>([])

  const navigate = useNavigate()
  const cellRefs = useRef<(HTMLParagraphElement | null)[]>([])
  const chunkSize = 50

  // For preventing TableList out of view when filterswrap on narrowing the view
  useEffect(() => {
    const updateHeight = () => {
      const filtersDiv = document.querySelector('.filters')
      const filtersHeight = filtersDiv ? filtersDiv.scrollHeight : 0
      document.documentElement.style.setProperty(
        '--filters-height',
        `${filtersHeight}px`
      )
    }

    updateHeight()
    window.addEventListener('resize', updateHeight)

    return () => window.removeEventListener('resize', updateHeight)
  }, [])

  useEffect(() => {
    setVisibleData([])
    setAllDataLoaded(false)
    if (Array.isArray(data)) {
      setVisibleData(data.slice(0, chunkSize)) // Loads the first 50 rows initially
    } else {
      console.error('Expected data to be an array but got:', data)
    }
    // setVisibleData(data.slice(0, chunkSize)) // Loads the first 50 rows initially
  }, [data])

  // Automatically loads more rows in chunks in the background after the initial render
  useEffect(() => {
    const loadMoreRowsAsync = async () => {
      if (isLoadingMore || allDataLoaded) return // Avoids loading if already loading or all data is loaded

      setIsLoadingMore(true)

      const offset = visibleData.length
      const newData = data.slice(offset, offset + chunkSize)

      if (newData.length === 0) {
        setAllDataLoaded(true)
      } else {
        setVisibleData((prevData) => [...prevData, ...newData])
      }

      setIsLoadingMore(false)
    }
    if (visibleData.length < data.length && !isLoadingMore) {
      const intervalId = setInterval(() => {
        loadMoreRowsAsync()
      }, 200) // Loads rows every 200ms asynchronously without blocking the UI

      return () => clearInterval(intervalId) // Cleans up interval on unmount or data changes
    }
  }, [visibleData, data, isLoadingMore, allDataLoaded])

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const isComponentFocused =
        document.activeElement?.closest('.tables-container')

      if (event.key === 'Escape' && isComponentFocused) {
        setSelectedRowIndex(null)
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [])

  // Sets loading state and checks if each table cell is overflowing to determine if a tooltip should be displayed for that cell
  useEffect(() => {
    if (isLoading) {
      setLoading(true)
    } else {
      setLoading(false)
      setTimeout(() => {
        const isOverflowing = Array.isArray(cellRefs.current)
          ? cellRefs.current.map((el) =>
              el ? el.scrollWidth > el.clientWidth : false
            )
          : []
        setOverflowState(isOverflowing)
      }, 0)
    }
  }, [visibleData, columns, isLoading])

  const handleRowClick = (index: number) => {
    setSelectedRowIndex((prevIndex) => (prevIndex === index ? null : index))
  }

  const renderCellContent = useCallback(
    (row: T, column: Column<T>, rowIndex: number) => {
      const handleTableClick = (db: string, table: string) => {
        navigate(`/import/${db}/${table}/settings`)
      }
      const handleConnectionNameClick = (connection: string) => {
        navigate(`/connection/${connection}`)
      }
      const handleAirflowNameClick = (type: string, dagName: string) => {
        navigate(`/airflow/${type}/${dagName}/settings`)
      }
      const handleExportConnectionClick = (
        connection: string,
        schema: string,
        targetTable: string
      ) => {
        navigate(`/export/${connection}/${schema}/${targetTable}/settings`)
      }

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

      if (column.accessor === 'forceString') {
        return (
          <>
            <p>
              {cellValue === 1
                ? 'True'
                : cellValue === 0
                ? 'False'
                : 'Default from Config'}
            </p>
          </>
        )
      }

      if (column.accessor === 'sensorSoftFail') {
        return (
          <>
            <p>{cellValue === 1 ? 'True' : 'False'}</p>
          </>
        )
      }

      if (
        column.accessor === 'includeInImport' ||
        column.accessor === 'includeInExport' ||
        column.accessor === 'includeInAirflow'
      ) {
        return (
          <>
            <p>{cellValue === true ? 'True' : 'False'}</p>
          </>
        )
      }

      if (!isExport && column.accessor === 'table') {
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

      if (column.accessor === 'targetTable') {
        return (
          <p
            ref={(el) => (cellRefs.current[rowIndex] = el)}
            onClick={() =>
              handleExportConnectionClick(
                String(row['connection' as keyof T]),
                String(row['targetSchema' as keyof T]),
                String(row['targetTable' as keyof T])
              )
            }
            className="clickable-table-name"
          >
            {String(cellValue)}
          </p>
        )
      }

      if (column.header === 'Connection Name') {
        return (
          <p
            ref={(el) => (cellRefs.current[rowIndex] = el)}
            onClick={() =>
              handleConnectionNameClick(String(row['name' as keyof T]))
            }
            className="clickable-table-name"
          >
            {String(cellValue)}
          </p>
        )
      }

      if (airflowType && column.header === 'DAG Name') {
        return (
          <p
            ref={(el) => (cellRefs.current[rowIndex] = el)}
            onClick={() =>
              handleAirflowNameClick(
                airflowType,
                String(row['name' as keyof T])
              )
            }
            className="clickable-table-name"
          >
            {String(cellValue)}
          </p>
        )
      }

      if (column.header === 'Task Name' || column.header === 'Column Name') {
        return (
          <p
            ref={(el) => (cellRefs.current[rowIndex] = el)}
            onClick={() => onEdit && onEdit(row, rowIndex)}
            className="clickable-table-name"
          >
            {String(cellValue)}
          </p>
        )
      }

      if (column.accessor === 'databaseType') {
        return (
          <p
            ref={(el) => (cellRefs.current[rowIndex] = el)}
            onClick={() => onEdit && onEdit(row, rowIndex)}
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
              <button
                onClick={() => onEdit && onEdit(row, rowIndex)}
                disabled={!onEdit}
              >
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
    },
    [isExport, airflowType, navigate, overflowState, onEdit]
  )

  return (
    <div className="tables-container" tabIndex={0}>
      {loading ? (
        <div className="loading-container">
          <p>Loading tables...</p>
        </div>
      ) : (
        <div
          className="scrollable-container"
          style={
            {
              '--scrollbar-margin-top': scrollbarMarginTop
            } as React.CSSProperties
          }
        >
          <table className="custom-table-root">
            <thead>
              <tr>
                {Array.isArray(columns) &&
                  columns.map((column, index) => (
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
              {visibleData &&
                Array.isArray(visibleData) &&
                visibleData.map((row, rowIndex) => (
                  <tr
                    key={rowIndex}
                    className={`dbtables-row ${
                      rowIndex === selectedRowIndex ? 'selected' : ''
                    }`}
                    onClick={() => handleRowClick(rowIndex)}
                  >
                    {Array.isArray(columns) &&
                      columns.map((column) => (
                        <td
                          key={`${rowIndex}-${String(column.accessor)}`}
                          className={
                            column.accessor === 'sourceTable'
                              ? 'fixed-width'
                              : ''
                          }
                        >
                          {renderCellContent(row, column, rowIndex)}
                        </td>
                      ))}
                  </tr>
                ))}
              {data.length === 0 && !isLoading && (
                <tr>
                  <td
                    colSpan={columns.length}
                    style={{
                      padding: 0
                    }}
                  >
                    <p
                      style={{
                        padding: ' 40px 50px 44px 50px',
                        margin: 0,
                        backgroundColor: 'white',
                        textAlign: 'center'
                      }}
                    >
                      No data matching.
                    </p>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
      {isLoadingMore && !allDataLoaded && (
        <div className="loading-more-indicator">
          <p>Loading more rows...</p>
        </div>
      )}
    </div>
  )
}

const MemoizedTableList = React.memo(TableList, (prevProps, nextProps) => {
  // Only re-renders if the filteredData (data) or other important props change
  return (
    prevProps.data === nextProps.data &&
    prevProps.isLoading === nextProps.isLoading &&
    prevProps.columns === nextProps.columns
  )
}) as typeof TableList

export default MemoizedTableList
