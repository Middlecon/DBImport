import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useMemo } from 'react'
import ChevronRight from '../assets/icons/ChevronRight'
import { useAtom } from 'jotai'
import {
  exportCnListFiltersAtom,
  importDbListFiltersAtom,
  isAirflowSubmenuActiveAtom,
  isDbDropdownReadyAtom,
  selectedExportConnectionAtom,
  selectedImportDatabaseAtom
} from '../atoms/atoms'
import './Breadcrumbs.scss'

const Breadcrumbs = () => {
  const location = useLocation()
  const navigate = useNavigate()
  const [, setSelectedDatabase] = useAtom(selectedImportDatabaseAtom)
  const [, setIsDbDropdownReady] = useAtom(isDbDropdownReadyAtom)
  const [, setSelectedImportFilters] = useAtom(importDbListFiltersAtom)

  const [, setSelectedExportConnection] = useAtom(selectedExportConnectionAtom)
  const [, setSelectedExportFilters] = useAtom(exportCnListFiltersAtom)

  const [, setIsAirflowSubmenuActive] = useAtom(isAirflowSubmenuActiveAtom)

  const capitalizeFirstLetter = (string: string) => {
    return string.charAt(0).toUpperCase() + string.slice(1)
  }

  const crumbs = useMemo(() => {
    const pathnames = location.pathname.split('/').filter((x) => x)
    const breadcrumbItems = [{ label: 'Home', path: '/' }]

    pathnames.forEach((pathname, index) => {
      const path = `/${pathnames.slice(0, index + 1).join('/')}`
      const label =
        index === 0
          ? capitalizeFirstLetter(pathname)
          : pathnames[0] === 'airflow' && index === 1
          ? capitalizeFirstLetter(pathname)
          : pathname

      breadcrumbItems.push({
        label,
        path
      })
    })

    return breadcrumbItems
  }, [location])

  const handleBreadcrumbClick = (path: string) => {
    if (path === '/import') {
      setIsDbDropdownReady(false)
      setSelectedDatabase(null)
      setSelectedImportFilters({})
    }
    if (path === '/') {
      setIsAirflowSubmenuActive(false)
    }

    if (path === '/export') {
      setIsDbDropdownReady(false)
      setSelectedExportConnection(null)
      setSelectedExportFilters({})
    }
    navigate(path)
  }

  return (
    <nav aria-label="breadcrumb">
      <ol className="breadcrumb">
        {crumbs.map((crumb, idx) => (
          <li
            key={idx}
            className={`breadcrumb-item ${
              idx === crumbs.length - 1 ? 'active' : ''
            }`}
          >
            {idx > 0 && <ChevronRight />}
            {/* Only shows chevron after the first item */}
            {idx === crumbs.length - 1 || idx >= 3 ? (
              <span>{crumb.label}</span> /* Current/last item is not a link */
            ) : crumb.label === 'Airflow' ? (
              <span>{crumb.label}</span>
            ) : (
              <Link
                to={crumb.path}
                onClick={(e) => {
                  e.preventDefault()
                  handleBreadcrumbClick(crumb.path)
                }}
              >
                {crumb.label}
              </Link>
            )}
          </li>
        ))}
      </ol>
    </nav>
  )
}

export default Breadcrumbs
