import '../_shared/TableDetailedView.scss'
import '../import/tableDetailed/settings/TableSettings.scss'
import '../../components/Loading.scss'
import { useGlobalConfig } from '../../utils/queries'
import { configGlobalCardRenderSettings } from '../../utils/cardRenderFormatting'
import CardConfig from './CardConfig'

function ConfigGlobalSettings() {
  const { data: globalConfig } = useGlobalConfig()

  if (!globalConfig) return <div className="loading">Loading...</div>
  console.log('globalConfig', globalConfig)
  const globalSettings = configGlobalCardRenderSettings(globalConfig)

  return (
    <div className="block-container" style={{ margin: 0 }}>
      <div className="block-container-2">
        <div>
          <div className="cards-container">
            <CardConfig
              title="Airflow Settings"
              settings={globalSettings.airflowConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="Disable Operations Settings"
              settings={globalSettings.disableOperationsConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="Misc Settings"
              settings={globalSettings.miscConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="HDFS"
              settings={globalSettings.HDFSConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="SQL Servers"
              settings={globalSettings.sqlServersConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="Import & Export"
              settings={globalSettings.importAndExportConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="Notifications"
              settings={globalSettings.notificationsConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="DBImport Servers"
              settings={globalSettings.dbimportServersConfigData}
              originalData={globalConfig}
            />
            <CardConfig
              title="Performance"
              settings={globalSettings.performanceConfigData}
              originalData={globalConfig}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export default ConfigGlobalSettings
