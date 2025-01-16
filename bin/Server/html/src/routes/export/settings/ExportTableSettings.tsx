import '../../_shared/tableDetailed/settings/TableSettings.scss'
import { useParams } from 'react-router-dom'
import Card from '../../_shared/tableDetailed/settings/Card'
import '../../../components/Loading.scss'
import { useExportTable } from '../../../utils/queries'
import { exportCardRenderSettings } from '../../../utils/cardRenderFormatting'

function ExportTableSettings() {
  const { connection, targetSchema, targetTable } = useParams()

  const { data: tableData, isError } = useExportTable(
    connection,
    targetSchema,
    targetTable
  )

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (!tableData && !isError) return <div className="loading">Loading...</div>
  const exportCards = exportCardRenderSettings(tableData)
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
                tableData={tableData}
              />
              <Card
                type="export"
                title="Performance"
                settings={exportCards.performance}
                tableData={tableData}
              />
              <Card
                type="export"
                title="Validation"
                settings={exportCards.validation}
                tableData={tableData}
              />
              <Card
                type="export"
                title="Schedule"
                settings={exportCards.schedule}
                tableData={tableData}
              />
            </div>
            <div className="cards-container">
              <Card
                type="export"
                title="Export Options"
                settings={exportCards.exportOptions}
                tableData={tableData}
              />

              <Card
                type="export"
                title="Incremental Exports"
                settings={exportCards.incrementalExports}
                tableData={tableData}
                isNotEditable={tableData.exportType !== 'incr'}
                isDisabled={tableData.exportType !== 'incr'}
              />
            </div>
          </div>
          <div className="cards-narrow">
            <div className="cards-container">
              <Card
                type="export"
                title="Main Settings"
                settings={exportCards.mainSettings}
                tableData={tableData}
              />
              <Card
                type="export"
                title="Export Options"
                settings={exportCards.exportOptions}
                tableData={tableData}
              />

              <Card
                type="export"
                title="Incremental Exports"
                settings={exportCards.incrementalExports}
                tableData={tableData}
                isNotEditable={tableData.exportType !== 'incr'}
                isDisabled={tableData.exportType !== 'incr'}
              />
              <Card
                type="export"
                title="Performance"
                settings={exportCards.performance}
                tableData={tableData}
              />
              <Card
                type="export"
                title="Validation"
                settings={exportCards.validation}
                tableData={tableData}
              />
              <Card
                type="export"
                title="Schedule"
                settings={exportCards.schedule}
                tableData={tableData}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default ExportTableSettings
