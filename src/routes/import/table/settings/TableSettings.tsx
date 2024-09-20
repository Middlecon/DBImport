import { useOutletContext } from 'react-router-dom'
import { UITable } from '../../../../utils/interfaces'
import './TableSettings.scss'
import CardsRenderer from './CardsRenderer'

function TableSettings() {
  const { data } = useOutletContext<{ data: UITable }>()
  // console.log('data', data)

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <CardsRenderer table={data} />
        </div>
      </div>
    </>
  )
}

export default TableSettings
