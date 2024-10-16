import './ViewBaseLayout.scss'
import ChevronRight from '../assets/icons/ChevronRight'
import { ReactNode } from 'react'

interface ViewLayoutProps {
  breadcrumbs: string[]
  children: ReactNode
}

function ViewLayout({ breadcrumbs = [], children }: ViewLayoutProps) {
  return (
    <>
      <div className="view-layout-root">
        <div className="fake-breadcrumbs">
          Home
          {Array.isArray(breadcrumbs) &&
            breadcrumbs.map((breadcrumb, index) => (
              <span key={index}>
                <ChevronRight />
                <span>{breadcrumb}</span>
              </span>
            ))}
        </div>

        {children}
      </div>
    </>
  )
}

export default ViewLayout
