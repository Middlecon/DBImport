import '../_shared/TableDetailedView.scss'

import ConfigurationGlobal from './ConfigGlobal'
import ViewBaseLayout from '../../components/ViewBaseLayout'

function ConfigGlobalBase() {
  return (
    <>
      <ViewBaseLayout>
        <div className="table-header">
          <h1>Global Configuration</h1>
        </div>
        <ConfigurationGlobal />
      </ViewBaseLayout>
    </>
  )
}

export default ConfigGlobalBase
