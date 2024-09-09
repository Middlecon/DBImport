import DropdownFiltered from '../components/DropdownFiltered'
import './Import.scss'

function Import() {
  const handleSelect = (item: string) => {
    console.log('Selected item:', item)
  }

  return (
    <>
      <div className="import-root">
        <h1>Import</h1>
        <div className="db-dropdown">
          <DropdownFiltered
            items={['DB1', 'DB2', 'DB3']}
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
