import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useMemo } from 'react'
import ChevronRight from '../assets/icons/ChevronRight'
import { useAtom } from 'jotai'
import {
  importDbListFiltersAtom,
  isDbDropdownReadyAtom,
  selectedImportDatabaseAtom
} from '../atoms/atoms'
import './Breadcrumbs.scss'

const Breadcrumbs = () => {
  const location = useLocation()
  const navigate = useNavigate()
  const [, setSelectedDatabase] = useAtom(selectedImportDatabaseAtom)
  const [, setIsDbDropdownReady] = useAtom(isDbDropdownReadyAtom)
  const [, setSelectedFilters] = useAtom(importDbListFiltersAtom)

  const capitalizeFirstLetter = (string: string) => {
    return string.charAt(0).toUpperCase() + string.slice(1)
  }

  const crumbs = useMemo(() => {
    const pathnames = location.pathname.split('/').filter((x) => x)

    const breadcrumbItems = []

    breadcrumbItems.push({ label: 'Home', path: '/' })

    if (pathnames[0]) {
      breadcrumbItems.push({
        label: capitalizeFirstLetter(pathnames[0]),
        path: `/${pathnames[0]}`
      })
    }

    if (pathnames[1]) {
      breadcrumbItems.push({
        label: pathnames[1],
        path: `/${pathnames[0]}/${pathnames[1]}`
      })
    }

    if (pathnames[2]) {
      breadcrumbItems.push({
        label: pathnames[2],
        path: `/${pathnames[0]}/${pathnames[1]}/${pathnames[2]}`
      })
    }

    return breadcrumbItems
  }, [location])

  const handleBreadcrumbClick = (path: string) => {
    if (path === '/import') {
      setIsDbDropdownReady(false)
      setSelectedDatabase(null)
      setSelectedFilters({})
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
            {idx === crumbs.length - 1 ? (
              <span>{crumb.label}</span> /* Current/last item is not a link */
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
