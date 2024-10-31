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
  const { data: table, isFetching } = useTable(database, tableParam)

  if (isFetching) return <div className="loading">Loading...</div>
  if (!table) return <div>No data found.</div>
  const importCards = importCardRenderSettings(table)

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div className="cards">
            <div className="cards-container">
              <Card
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
              />
            </div>
            <div className="cards-container">
              <Card
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
              />
            </div>
          </div>
          <div className="cards-narrow">
            <div className="cards-container">
              <Card
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
                isNotEditable={table.importPhaseType !== 'incr'}
                isDisabled={table.importPhaseType !== 'incr'}
              />
              <Card
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
                isNotEditable={true}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default TableSettings
