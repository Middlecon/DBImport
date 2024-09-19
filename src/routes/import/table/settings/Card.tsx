import React from 'react'
import Field from './Field'
import { FieldType } from '../../../../utils/enums'

interface CardProps {
  title: string
  fields: Array<{
    label: string
    value: string | number | boolean
    type: FieldType
    enumOptions?: { [key: string]: string } // Maybe not needed here
    hidden?: boolean
  }>
}

const Card: React.FC<CardProps> = ({ title, fields }) => {
  return (
    <div className="card">
      <h3>{title}</h3>
      {fields.map((field, idx) => (
        <Field key={idx} {...field} />
      ))}
    </div>
  )
}

export default Card
