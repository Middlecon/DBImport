import './TableSettings.scss'
import { useParams } from 'react-router-dom'
import { useTable } from '../../../../utils/queries'
import Card from './Card'
import { importCardRenderSettings } from '../../../../utils/cardRenderFormatting'
import '../../../../components/Loading.scss'

function TableSettings() {
  const { database, table: tableParam } = useParams<{
    database: string
    table: string
  }>()
  const { data: table } = useTable(database, tableParam)

  if (!table) return <div className="loading">Loading...</div>

  const importCards = importCardRenderSettings(table)

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div className="cards">
            <div className="cards-container">
              <Card
                type="import"
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                type="import"
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                type="import"
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                type="import"
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                type="import"
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
              />
            </div>
            <div className="cards-container">
              <Card
                type="import"
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
                isNotEditable={table.importPhaseType !== 'incr'}
                isDisabled={table.importPhaseType !== 'incr'}
              />
            </div>
          </div>
          <div className="cards-narrow">
            <div className="cards-container">
              <Card
                type="import"
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                type="import"
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
                isNotEditable={table.importPhaseType !== 'incr'}
                isDisabled={table.importPhaseType !== 'incr'}
              />
              <Card
                type="import"
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                type="import"
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                type="import"
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                type="import"
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default TableSettings
