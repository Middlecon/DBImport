import './RequiredFieldsInfo.scss'

function RequiredFieldsInfo({
  isRequiredFieldEmpty
}: {
  isRequiredFieldEmpty: boolean
}) {
  return (
    <p
      className={
        isRequiredFieldEmpty ? 'required-info' : 'disabled-required-info'
      }
    >
      *
      <span style={{ fontSize: 11, marginLeft: 2 }}>
        = required field, needs to be filled before save
      </span>
    </p>
  )
}

export default RequiredFieldsInfo
