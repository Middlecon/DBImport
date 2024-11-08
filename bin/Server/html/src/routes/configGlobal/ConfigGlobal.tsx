import '../_shared/TableDetailedView.scss'
import '../import/tableDetailed/settings/TableSettings.scss'
import '../../components/Loading.scss'
import { useGlobalConfig } from '../../utils/queries'
import { configGlobalCardRenderSettings } from '../../utils/cardRenderFormatting'
import CardConfig from './CardConfig'
import { EditSetting } from '../../utils/interfaces'

function ConfigGlobalSettings() {
  const { data: globalConfig, isError } = useGlobalConfig()

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (!globalConfig && !isError) {
    return <div className="loading">Loading...</div>
  }

  console.log('globalConfig', globalConfig)
  const globalSettings = configGlobalCardRenderSettings(globalConfig)

  const shouldRenderCard = (settings: EditSetting[]) => {
    return (
      settings &&
      settings.length > 0 &&
      !settings.every((setting) => setting.isHidden)
    )
  }

  return (
    <div className="block-container" style={{ margin: 0 }}>
      <div className="block-container-2">
        <div className="cards">
          <div className="cards-container">
            {shouldRenderCard(globalSettings.airflowConfigData) && (
              <CardConfig
                title="Airflow Settings"
                settings={globalSettings.airflowConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.miscConfigData) && (
              <CardConfig
                title="Misc Settings"
                settings={globalSettings.miscConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.sqlServersConfigData) && (
              <CardConfig
                title="SQL Servers"
                settings={globalSettings.sqlServersConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.notificationsConfigData) && (
              <CardConfig
                title="Notifications"
                settings={globalSettings.notificationsConfigData}
                originalData={globalConfig}
              />
            )}
          </div>

          <div className="cards-container">
            {shouldRenderCard(globalSettings.disableOperationsConfigData) && (
              <CardConfig
                title="Disable Operations Settings"
                settings={globalSettings.disableOperationsConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.HDFSConfigData) && (
              <CardConfig
                title="HDFS"
                settings={globalSettings.HDFSConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.importAndExportConfigData) && (
              <CardConfig
                title="Import & Export"
                settings={globalSettings.importAndExportConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.dbimportServersConfigData) && (
              <CardConfig
                title="DBImport Servers"
                settings={globalSettings.dbimportServersConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.performanceConfigData) && (
              <CardConfig
                title="Performance"
                settings={globalSettings.performanceConfigData}
                originalData={globalConfig}
              />
            )}
          </div>
        </div>
        <div className="cards-narrow">
          <div className="cards-container">
            {shouldRenderCard(globalSettings.airflowConfigData) && (
              <CardConfig
                title="Airflow Settings"
                settings={globalSettings.airflowConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.disableOperationsConfigData) && (
              <CardConfig
                title="Disable Operations Settings"
                settings={globalSettings.disableOperationsConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.miscConfigData) && (
              <CardConfig
                title="Misc Settings"
                settings={globalSettings.miscConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.HDFSConfigData) && (
              <CardConfig
                title="HDFS"
                settings={globalSettings.HDFSConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.sqlServersConfigData) && (
              <CardConfig
                title="SQL Servers"
                settings={globalSettings.sqlServersConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.importAndExportConfigData) && (
              <CardConfig
                title="Import & Export"
                settings={globalSettings.importAndExportConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.notificationsConfigData) && (
              <CardConfig
                title="Notifications"
                settings={globalSettings.notificationsConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.dbimportServersConfigData) && (
              <CardConfig
                title="DBImport Servers"
                settings={globalSettings.dbimportServersConfigData}
                originalData={globalConfig}
              />
            )}
            {shouldRenderCard(globalSettings.performanceConfigData) && (
              <CardConfig
                title="Performance"
                settings={globalSettings.performanceConfigData}
                originalData={globalConfig}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default ConfigGlobalSettings
