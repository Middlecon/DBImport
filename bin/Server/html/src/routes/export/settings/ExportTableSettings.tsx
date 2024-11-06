import '../../import/tableDetailed/settings/TableSettings.scss'
import { useParams } from 'react-router-dom'
import Card from '../../import/tableDetailed/settings/Card'
import '../../../components/Loading.scss'
import { useExportTable } from '../../../utils/queries'
import { exportCardRenderSettings } from '../../../utils/cardRenderFormatting'

function ExportTableSettings() {
  const { connection, schema, table } = useParams()

  const { data: exportTableData } = useExportTable(connection, schema, table)

  if (!exportTableData) return <div className="loading">Loading...</div>
  const exportCards = exportCardRenderSettings(exportTableData)
  console.log('exportTableData', exportTableData)
  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div className="cards">
            <div className="cards-container">
              <Card
                type="export"
                title="Main Settings"
                settings={exportCards.mainSettings}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Performance"
                settings={exportCards.performance}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Validation"
                settings={exportCards.validation}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Schedule"
                settings={exportCards.schedule}
                tableData={exportTableData}
              />
            </div>
            <div className="cards-container">
              <Card
                type="export"
                title="Export Options"
                settings={exportCards.exportOptions}
                tableData={exportTableData}
              />

              <Card
                type="export"
                title="Incremental Exports"
                settings={exportCards.incrementalExports}
                tableData={exportTableData}
                isNotEditable={exportTableData.exportType !== 'incr'}
                isDisabled={exportTableData.exportType !== 'incr'}
              />
            </div>
          </div>
          <div className="cards-narrow">
            <div className="cards-container">
              <Card
                type="export"
                title="Main Settings"
                settings={exportCards.mainSettings}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Export Options"
                settings={exportCards.exportOptions}
                tableData={exportTableData}
              />

              <Card
                type="export"
                title="Incremental Exports"
                settings={exportCards.incrementalExports}
                tableData={exportTableData}
                isNotEditable={exportTableData.exportType !== 'incr'}
                isDisabled={exportTableData.exportType !== 'incr'}
              />
              <Card
                type="export"
                title="Performance"
                settings={exportCards.performance}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Validation"
                settings={exportCards.validation}
                tableData={exportTableData}
              />
              <Card
                type="export"
                title="Schedule"
                settings={exportCards.schedule}
                tableData={exportTableData}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default ExportTableSettings
