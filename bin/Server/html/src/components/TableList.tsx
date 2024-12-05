import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  useReactTable,
  getCoreRowModel,
  getFilteredRowModel,
  Row,
  flexRender,
  Table as ReactTable,
  ColumnDef
} from '@tanstack/react-table'
import { Column } from '../utils/interfaces'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'
import EditIcon from '../assets/icons/EditIcon'
import DeleteIcon from '../assets/icons/DeleteIcon'
import './TableList.scss'

interface TableProps<T> {
  columns: Column<T>[]
  data: T[]
  isLoading: boolean
  onEdit?: (row: T, rowIndex?: number) => void
  onBulkEdit?: (rows: T[], rowsIndex?: number[]) => void
  onDelete?: (row: T) => void
  airflowType?: string
  isExport?: boolean
}

function TableList<T>({
  columns,
  data,
  isLoading,
  onEdit,
  onDelete,
  airflowType,
  isExport = false
}: TableProps<T>) {
  const tableHeaderRef = useRef<HTMLDivElement | null>(null)
  const [scrollbarMarginTop, setScrollbarMarginTop] = useState<string>('')
  const [isScrolledToEnd, setIsScrolledToEnd] = useState(false)

  useEffect(() => {
    const updateScrollbarMarginTop = () => {
      if (tableHeaderRef.current) {
        const headerHeight = tableHeaderRef.current.offsetHeight
        setScrollbarMarginTop(`${headerHeight}px`)
      }
    }

    const handleResize = customDebounce(() => {
      updateScrollbarMarginTop()
    }, 100)

    // Initial calculation
    updateScrollbarMarginTop()

    const resizeObserver = new ResizeObserver(() => {
      updateScrollbarMarginTop()
    })

    const tableHeaderRefCurrent = tableHeaderRef.current

    if (tableHeaderRefCurrent) {
      resizeObserver.observe(tableHeaderRefCurrent)
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      if (tableHeaderRefCurrent) {
        resizeObserver.unobserve(tableHeaderRefCurrent)
      }
      resizeObserver.disconnect()
    }
  }, [])

  function customDebounce(func: () => void, wait: number) {
    let timeout: NodeJS.Timeout | null = null
    return () => {
      if (timeout) clearTimeout(timeout)
      timeout = setTimeout(func, wait)
    }
  }

  useEffect(() => {
    const handleScroll = () => {
      if (scrollableRef.current) {
        const { scrollLeft, scrollWidth, clientWidth } = scrollableRef.current
        const atRightEnd = Math.abs(scrollLeft + clientWidth - scrollWidth) <= 1
        setIsScrolledToEnd(atRightEnd)
      }
    }

    const scrollableContainer = scrollableRef.current
    if (scrollableContainer) {
      scrollableContainer.addEventListener('scroll', handleScroll)
    }

    return () => {
      if (scrollableContainer) {
        scrollableContainer.removeEventListener('scroll', handleScroll)
      }
    }
  }, [])

  const navigate = useNavigate()

  const [selectedRowIds, setSelectedRowIds] = useState<Record<string, boolean>>(
    {}
  )
  const [lastSelectedRowIndex, setLastSelectedRowIndex] = useState<
    number | null
  >(null)

  const cellRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const scrollableRef = useRef<HTMLDivElement | null>(null)

  const [overflowState, setOverflowState] = useState<boolean[]>([])
  const [loading, setLoading] = useState(false)

  const preventBackNavigationOnScroll = (event: WheelEvent) => {
    const scrollableContainer = event.currentTarget as HTMLElement

    if (!scrollableContainer) return

    const { scrollLeft, scrollWidth, clientWidth } = scrollableContainer

    const isAtRightEnd = scrollLeft + clientWidth >= scrollWidth

    if (isAtRightEnd && event.deltaX > 0) {
      // Prevents scrolling further right
      event.preventDefault()
      event.stopPropagation()
    }
  }

  const attachPreventScrollListener = () => {
    const scrollableContainer = document.querySelector(
      '.scrollable-container'
    ) as HTMLElement

    if (scrollableContainer) {
      scrollableContainer.addEventListener(
        'wheel',
        preventBackNavigationOnScroll,
        {
          passive: false // Allows preventDefault to block scrolling
        }
      )
    }
  }

  // Calls this function once the DOM is ready
  attachPreventScrollListener()

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
    if (isLoading) {
      setLoading(true)
    } else {
      setLoading(false)

      // Check overflow for all cells in `cellRefs`
      const isOverflowing = Object.values(cellRefs.current).map((el) =>
        el ? el.scrollWidth > el.clientWidth : false
      )

      setOverflowState(isOverflowing)
    }
  }, [isLoading])

  // // Sets loading state and checks if each table cell is overflowing
  // useEffect(() => {
  //   if (isLoading) {
  //     setLoading(true)
  //   } else {
  //     setLoading(false)
  //     setTimeout(() => {
  //       const isOverflowing = Array.isArray(cellRefs.current)
  //         ? cellRefs.current.map((el) =>
  //             el ? el.scrollWidth > el.clientWidth : false
  //           )
  //         : []
  //       setOverflowState(isOverflowing)
  //     }, 0)
  //   }
  // }, [columns, isLoading])

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

  // Define columns for TanStack Table
  const tanstackColumns = useMemo<ColumnDef<T>[]>(
    () =>
      columns.map((column) => ({
        id: (column.accessor ||
          column.header.toLowerCase().replace(/\s+/g, '-')) as string,
        accessorKey: column.accessor as string,
        header: column.header,
        size: 150, // Default column width
        minSize: 20,
        enableResizing: true,
        meta: {
          isSticky: column.header === 'Actions' || column.header === 'Links',
          stickyType:
            column.header === 'Actions'
              ? 'actions'
              : column.header === 'Links'
              ? 'links'
              : undefined
        },

        cell: (info) => (
          <div
            style={{
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis'
            }}
            // title={String(info.getValue())}
          >
            {renderCellContent(info.row.original, column, info.row.index)}
          </div>
        )
        //   // renderCellContent(info.row.original, column, info.row.index)
      })),
    [columns, renderCellContent]
  )

  const table = useReactTable({
    data,
    columns: tanstackColumns,
    state: {
      rowSelection: selectedRowIds,
      columnSizing: {}
    },
    // onColumnSizingChange: (newSizing) => {
    //   console.log('New column sizing:', newSizing)
    // },
    onRowSelectionChange: setSelectedRowIds,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    enableRowSelection: true,
    columnResizeMode: 'onChange',
    defaultColumn: {
      minSize: 20
    }
  })

  // Handles row click for selection
  const handleRowClick = useCallback(
    (event: React.MouseEvent, row: Row<T>) => {
      event.preventDefault() // Prevents default text selection behavior

      const clickedRowIndex = row.index

      if (event.ctrlKey || event.metaKey) {
        // Toggles selection
        setSelectedRowIds((prev) => ({
          ...prev,
          [row.id]: !prev[row.id]
        }))
        setLastSelectedRowIndex(clickedRowIndex)
      } else if (event.shiftKey) {
        if (lastSelectedRowIndex === null) {
          // If no previous selection, select only the clicked row
          setSelectedRowIds({ [row.id]: true })
          setLastSelectedRowIndex(clickedRowIndex)
        } else {
          const start = Math.min(lastSelectedRowIndex, clickedRowIndex)
          const end = Math.max(lastSelectedRowIndex, clickedRowIndex)

          const rowsInRange = table.getRowModel().rows.slice(start, end + 1)
          const newSelectedRowIds = { ...selectedRowIds }

          rowsInRange.forEach((r) => {
            newSelectedRowIds[r.id] = true
          })

          setSelectedRowIds(newSelectedRowIds)
        }
      } else {
        // Select only the clicked row
        setSelectedRowIds({ [row.id]: true })
        setLastSelectedRowIndex(clickedRowIndex)
      }
    },
    [lastSelectedRowIndex, selectedRowIds, table]
  )

  // Calculate column sizes and set CSS variables
  const columnSizeVars = useMemo(() => {
    const headers = table.getFlatHeaders()
    const colSizes: { [key: string]: number } = {}
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i]!
      colSizes[`--header-${header.id}-size`] = header.getSize()
      colSizes[`--col-${header.column.id}-size`] = header.column.getSize()
    }
    return colSizes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [table.getState().columnSizing])

  // Handles Ctrl+A / Cmd+A and Escape keys
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key === 'a') {
        event.preventDefault()
        const allRowIds = table.getRowModel().rows.map((row) => row.id)
        setSelectedRowIds(
          allRowIds.reduce((prev, id) => {
            prev[id] = true
            return prev
          }, {} as Record<string, boolean>)
        )
      } else if (event.key === 'Escape') {
        setSelectedRowIds({})
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [table])

  useEffect(() => {
    const updateSizing = (columnId: string, size: number) => {
      table.setColumnSizing((prevSizing) => ({
        ...prevSizing,
        [columnId]: size
      }))
    }

    tanstackColumns.forEach((column) => {
      updateSizing(column.id!, column.size!)
    })
  }, [tanstackColumns, table])

  // Determines the sticky column for the box-shadow
  const getBoxShadowColumnIndex = () => {
    const linkColumnIndex = tanstackColumns.findIndex(
      (col) => col.meta?.stickyType === 'links'
    )
    const actionColumnIndex = tanstackColumns.findIndex(
      (col) => col.meta?.stickyType === 'actions'
    )

    if (linkColumnIndex !== -1) return linkColumnIndex // Links get priority
    if (actionColumnIndex !== -1) return actionColumnIndex // Fallback to Actions

    return null // No shadow column
  }

  const boxShadowColumnIndex = getBoxShadowColumnIndex()

  // Memoized TableBody for performance during resizing
  const TableBody = useCallback(
    ({ table }: { table: ReactTable<T> }) => {
      return (
        <div className="tbody">
          {table.getRowModel().rows.map((row) => (
            <div
              key={row.id}
              className={`tr dbtables-row ${
                row.getIsSelected() ? 'selected' : ''
              }`}
              onClick={(event) => handleRowClick(event, row)}
            >
              {row.getVisibleCells().map((cell, index) => {
                const isBoxShadowApplied = index === boxShadowColumnIndex
                return (
                  <div
                    key={cell.id}
                    className={`td ${
                      cell.column.id === 'sourceTable' ? 'fixed-width' : ''
                    } ${
                      cell.column.columnDef.meta?.isSticky
                        ? `sticky-right ${
                            cell.column.columnDef.meta?.stickyType
                          } ${
                            isBoxShadowApplied && !isScrolledToEnd
                              ? 'has-shadow'
                              : ''
                          }`
                        : ''
                    }`}
                    style={{
                      width: `calc(var(--header-${cell.id}-size) * 1px)`
                    }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </div>
                )
              })}
            </div>
          ))}
        </div>
      )
    },
    [boxShadowColumnIndex, handleRowClick, isScrolledToEnd]
  )

  const MemoizedTableBody = React.useMemo(
    () =>
      React.memo(
        TableBody,
        (prevProps, nextProps) =>
          prevProps.table.options.data === nextProps.table.options.data
      ),
    [TableBody]
  )

  // State to control whether to use memoization
  // const [enableMemo, setEnableMemo] = useState(true)

  return (
    <div className="tables-container sticky-container" tabIndex={0}>
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
              ...columnSizeVars,
              '--scrollbar-margin-top': scrollbarMarginTop
            } as React.CSSProperties
          }
        >
          <div
            className="divTable"
            style={{
              width: table.getTotalSize()
            }}
          >
            <div className="thead" ref={tableHeaderRef}>
              {table.getHeaderGroups().map((headerGroup) => (
                <div key={headerGroup.id} className="tr">
                  {headerGroup.headers.map((header, index) => {
                    const isBoxShadowApplied = index === boxShadowColumnIndex

                    return (
                      <div
                        key={header.id}
                        className={`th ${
                          header.id === 'sourceTable' ? 'fixed-width' : ''
                        }
                        ${
                          header.id === 'serverType'
                            ? 'server-type-fixed-width'
                            : ''
                        } ${
                          header.column.columnDef.meta?.isSticky
                            ? `sticky-right ${
                                header.column.columnDef.meta?.stickyType
                              } ${
                                isBoxShadowApplied && !isScrolledToEnd
                                  ? 'has-shadow'
                                  : ''
                              }`
                            : ''
                        } ${header.column.getIsResizing() ? 'isResizing' : ''}`}
                        style={{
                          width: `calc(var(--header-${header.id}-size) * 1px)`
                        }}
                      >
                        {header.isPlaceholder
                          ? null
                          : flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                        {header.column.getCanResize() && (
                          <div
                            onDoubleClick={() => header.column.resetSize()}
                            onMouseDown={header.getResizeHandler()}
                            onTouchStart={header.getResizeHandler()}
                            className={`resizer ${
                              header.column.getIsResizing() ? 'isResizing' : ''
                            }`}
                          />
                        )}
                      </div>
                    )
                  })}
                </div>
              ))}
            </div>
            {/* Use memoized TableBody during resizing for performance */}
            {table.getState().columnSizingInfo.isResizingColumn ? (
              // && enableMemo
              <MemoizedTableBody
                table={table}
                // handleRowClick={handleRowClick}
              />
            ) : (
              <TableBody
                table={table}
                // handleRowClick={handleRowClick}
              />
            )}
          </div>
        </div>
      )}
    </div>
  )
}

interface TableBodyProps<T> {
  table: ReactTable<T>
  handleRowClick: (event: React.MouseEvent, row: Row<T>) => void
}

function TableBody<T>({ table, handleRowClick }: TableBodyProps<T>) {
  return (
    <div className="tbody">
      {table.getRowModel().rows.map((row) => (
        <div
          key={row.id}
          className={`tr dbtables-row ${row.getIsSelected() ? 'selected' : ''}`}
          onClick={(event) => handleRowClick(event, row)}
        >
          {row.getVisibleCells().map((cell) => (
            <div
              key={cell.id}
              className="td"
              style={{
                width: `calc(var(--col-${cell.column.id}-size) * 1px)`
              }}
            >
              {flexRender(cell.column.columnDef.cell, cell.getContext())}
              {cell.column.columnDef.meta?.stickyType}
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

export const MemoizedTableBody = React.memo(
  TableBody,
  (prevProps, nextProps) => {
    return prevProps.table.options.data === nextProps.table.options.data
  }
) as typeof TableBody

export default TableList
