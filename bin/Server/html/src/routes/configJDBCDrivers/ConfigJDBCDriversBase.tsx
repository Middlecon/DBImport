import '../_shared/TableDetailedView.scss'

import ViewBaseLayout from '../../components/ViewBaseLayout'
import ConfigJDBCDrivers from './ConfigJDBCDrivers'

function ConfigJDBCDriversBase() {
  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>JDBC Drivers</h1>
        </div>
        <ConfigJDBCDrivers />
      </ViewBaseLayout>
    </>
  )
}

export default ConfigJDBCDriversBase
