import React, { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import { Column } from '../utils/interfaces'
import './TableList.scss'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'

interface TableProps<T> {
  columns: Column<T>[]
  data: T[]
  isLoading: boolean
  onEdit?: (row: T, rowIndex?: number) => void
  onDelete?: (row: T) => void
  scrollbarMarginTop?: string
  airflowType?: string
  isExport?: boolean
}

function TableList<T>({
  columns,
  data,
  isLoading,
  onEdit,
  onDelete,
  scrollbarMarginTop,
  airflowType,
  isExport = false
}: TableProps<T>) {
  const hasAction = columns.some((column) => column.isAction)
  const hasLink = columns.some((column) => column.isLink)
  const hasActionAndLink = hasAction && hasLink

  const [selectedRowIndex, setSelectedRowIndex] = useState<number | null>(null)
  const [visibleData, setVisibleData] = useState<T[]>([])
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [allDataLoaded, setAllDataLoaded] = useState(false)
  const [loading, setLoading] = useState(false)

  const [overflowState, setOverflowState] = useState<boolean[]>([])

  const navigate = useNavigate()
  const cellRefs = useRef<(HTMLParagraphElement | null)[]>([])
  const chunkSize = 50

  const [isAtRightEnd, setIsAtRightEnd] = useState(false)
  const scrollableRef = useRef<HTMLDivElement | null>(null)

  const handleScroll = () => {
    if (scrollableRef.current) {
      const { scrollLeft, scrollWidth, clientWidth } = scrollableRef.current

      const atRightEnd = scrollLeft + clientWidth >= scrollWidth
      setIsAtRightEnd(atRightEnd)
    }
  }

  useEffect(() => {
    const container = scrollableRef.current
    if (container) {
      container.addEventListener('scroll', handleScroll)
      return () => container.removeEventListener('scroll', handleScroll)
    }
  }, [])

  const preventBackNavigationOnScroll = (event: WheelEvent) => {
    const container = event.currentTarget as HTMLElement
    const isAtLeftEdge = container.scrollLeft === 0 && event.deltaX < 0

    if (isAtLeftEdge) {
      event.preventDefault() // Prevents the default browser action (back navigation).
    }
  }

  const scrollableContainer = document.querySelector(
    '.scrollable-container'
  ) as HTMLElement

  if (scrollableContainer) {
    scrollableContainer.addEventListener(
      'wheel',
      preventBackNavigationOnScroll,
      {
        passive: false // Allows `preventDefault()` to work
      }
    )
  }

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
      const handleTableClick = (database: string, table: string) => {
        const encodedDatabase = encodeURIComponent(database)
        const encodedTable = encodeURIComponent(table)
        navigate(`/import/${encodedDatabase}/${encodedTable}/settings`)
      }
      const handleConnectionNameClick = (connection: string) => {
        const encodedConnection = encodeURIComponent(connection)
        navigate(`/connection/${encodedConnection}`)
      }
      const handleAirflowNameClick = (type: string, dagName: string) => {
        const encodedType = encodeURIComponent(type)
        const encodedDagName = encodeURIComponent(dagName)
        navigate(`/airflow/${encodedType}/${encodedDagName}/settings`)
      }
      const handleExportConnectionClick = (
        connection: string,
        targetSchema: string,
        targetTable: string
      ) => {
        const encodedConnection = encodeURIComponent(connection)
        const encodedTargetSchema = encodeURIComponent(targetSchema)
        const encodedTargetTable = encodeURIComponent(targetTable)
        navigate(
          `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/settings`
        )
      }

      const handleConnectionLinkClick = (
        type: 'import' | 'export',
        name: string
      ) => {
        const encodedName = encodeURIComponent(name)

        if (type === 'import') {
          navigate(`/import?connection=${encodedName}`)
        } else {
          navigate(`/export?connection=${encodedName}`)
        }
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
            className="clickable-name"
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
            className="clickable-name"
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
            className="clickable-name"
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
            className="clickable-name"
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
            className="clickable-name"
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
            className="clickable-name"
          >
            {String(cellValue)}
          </p>
        )
      }

      if (column.isLink) {
        return (
          <div className="actions-row">
            {column.isLink === 'connectionLink' ? (
              <>
                <button
                  className="actions-cn-link-button"
                  onClick={() =>
                    handleConnectionLinkClick(
                      'import',
                      String(row['name' as keyof T])
                    )
                  }
                >
                  <ImportIcon />
                </button>
                <button
                  className="actions-cn-link-button"
                  onClick={() =>
                    handleConnectionLinkClick(
                      'export',
                      String(row['name' as keyof T])
                    )
                  }
                >
                  <ExportIcon />
                </button>
              </>
            ) : null}
          </div>
        )
      }

      if (column.isAction) {
        return (
          <div className="actions-row">
            {column.isAction === 'edit' ||
            column.isAction === 'editAndDelete' ? (
              <button
                onClick={() => onEdit && onEdit(row, rowIndex)}
                disabled={!onEdit}
              >
                <EditIcon />
              </button>
            ) : null}
            {column.isAction === 'delete' ||
            column.isAction === 'editAndDelete' ? (
              <button
                className="actions-delete-button"
                onClick={() => onDelete && onDelete(row)}
                style={column.isAction === 'delete' ? { paddingLeft: 0 } : {}}
              >
                <DeleteIcon />
              </button>
            ) : null}
          </div>
        )
      }

      return String(cellValue)
    },
    [isExport, airflowType, navigate, overflowState, onEdit, onDelete]
  )

  return (
    <div className="tables-container" tabIndex={0}>
      {loading ? (
        <div className="loading-container">
          <p>Loading tables...</p>
        </div>
      ) : (
        <div
          ref={scrollableRef}
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
                      className={`${
                        column.accessor === 'sourceTable' ? 'fixed-width' : ''
                      }
                      ${
                        column.accessor === 'serverType'
                          ? 'server-type-fixed-width'
                          : ''
                      }
                      ${column.isAction ? 'sticky-right actions' : ''}
                      ${column.isLink ? 'sticky-right links' : ''}`}
                      style={{
                        boxShadow: isAtRightEnd
                          ? 'none'
                          : hasActionAndLink && column.isLink
                          ? '-20px 0 20px -15px rgba(0, 0, 0, .4)'
                          : hasAction && !hasLink && column.isAction
                          ? '-20px 0 20px -15px rgba(0, 0, 0, 0.1)'
                          : undefined
                      }}
                    >
                      <div
                        className={`${
                          column.isAction || column.isLink
                            ? 'sticky-th-wrapper'
                            : ''
                        }`}
                      >
                        {column.header}
                      </div>
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
                      columns.map((column, columnIndex) => (
                        <td
                          key={`${rowIndex}-${columnIndex}-${String(
                            column.accessor
                          )}`}
                          className={`${
                            column.accessor === 'sourceTable'
                              ? 'fixed-width'
                              : ''
                          }
                           ${column.isAction ? 'sticky-right actions' : ''}
                           ${column.isLink ? 'sticky-right links' : ''}`}
                          style={{
                            ...(column.isLink === 'connectionLink'
                              ? {
                                  width: 100,
                                  paddingLeft: 0,
                                  paddingRight: 0,
                                  border: 'none'
                                }
                              : column.isAction || column.isLink
                              ? {
                                  borderRight: 'none',
                                  borderLeft: 'none'
                                }
                              : {
                                  borderTop: '1px solid #ddd',
                                  borderRight: 'none'
                                }),
                            boxShadow: isAtRightEnd
                              ? 'none'
                              : hasActionAndLink && column.isLink
                              ? '-20px 0 20px -15px rgba(0, 0, 0, .8)'
                              : hasAction && !hasLink && column.isAction
                              ? '-20px 0 20px -15px rgba(0, 0, 0, .8)'
                              : undefined
                          }}
                        >
                          <div
                            className={`${
                              column.isAction || column.isLink
                                ? 'sticky-wrapper'
                                : ''
                            }`}
                          >
                            {renderCellContent(row, column, rowIndex)}
                          </div>
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
                        padding: '40px 50px 44px 50px',
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
