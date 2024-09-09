import DropdownFiltered from '../components/DropdownFiltered'
import { useDatabases } from '../utils/queries'
import './Import.scss'

function Import() {
  const { data } = useDatabases()
  const databaseNames: string[] = data?.map((db) => db.name) ?? []

  console.log('databases data', data)

  const handleSelect = (item: string) => {
    console.log('Selected item:', item)
  }

  console.log('databases names', databaseNames)

  return (
    <>
      <div className="import-root">
        <h1>Import</h1>
        <div className="db-dropdown">
          <DropdownFiltered
            items={databaseNames.length > 0 ? databaseNames : ['No DB yet']}
            initialTitle="Select DB"
            onSelect={handleSelect}
          />
        </div>
        <div className="import-text-block">
          <p>
            Please select a DB in the above dropdown to show and edit settings
            for its tables.
          </p>
        </div>
      </div>
    </>
  )
}

export default Import
