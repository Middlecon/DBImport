import { SettingType } from '../../../../utils/enums'
import Setting from './Setting'
import './Card.scss'

interface CardProps {
  title: string
  settings: Array<{
    label: string
    value: string | number | boolean
    type: SettingType
    isConditionsMet?: boolean
    enumOptions?: { [key: string]: string } // Maybe not needed here
    isHidden?: boolean
  }>
}

const Card: React.FC<CardProps> = ({ title, settings }) => {
  return (
    <div className="card">
      <h3>{title}</h3>
      <dl>
        {settings.map((setting, idx) => (
          <Setting key={idx} {...setting} />
        ))}
      </dl>
    </div>
  )
}

export default Card
