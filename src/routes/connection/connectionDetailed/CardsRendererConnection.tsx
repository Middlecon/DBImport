import { SettingType } from '../../../utils/enums'
import { TableSetting } from '../../../utils/interfaces'
import { useConnection } from '../../../utils/queries'
import '../../import/tableDetailed/settings/CardsRenderer.scss'
import { useParams } from 'react-router-dom'
import CardConnection from './CardConnection'

function CardsRendererConnection() {
  const { connection: connectionParam } = useParams<{
    connection: string
  }>()
  const { data: connection, isFetching } = useConnection(connectionParam)

  if (isFetching) return <div>Loading...</div>
  if (!connection) return <div>No data found.</div>
  console.log('connection', connection)

  const connectionSettings: TableSetting[] = [
    // { label: 'Name', value: connection.name, type: SettingType.Text }, //Free-text (varchar 256)
    {
      label: 'Connection String',
      value: connection.connectionString,
      type: SettingType.Textarea
    }, // Free-text (64k)
    {
      label: 'Private Key Path',
      value: connection.privateKeyPath,
      type: SettingType.Text
    }, // Free-text (varchar 128)
    {
      label: 'Public Key Path',
      value: connection.publicKeyPath,
      type: SettingType.Text
    }, // Free-text (varchar 128)
    {
      label: 'Credentials',
      value: connection.credentials !== null ? connection.credentials : '',
      type: SettingType.Textarea
    }, // Free-text (64k)

    {
      label: '',
      value: '',
      type: SettingType.GroupingSpace
    }, // Layout space
    {
      label: 'Source',
      value: connection.source,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    {
      label: 'Force String',
      value: connection.forceString,
      type: SettingType.BooleanOrDefaultFromConfig
    }, // Boolean, (1, 0) or Default from Config (-1)
    {
      label: 'Max Sessions',
      value: connection.maxSessions,
      type: SettingType.IntegerFromOneOrNull
    }, // Integer
    {
      label: 'Create Foreign Key',
      value: connection.createForeignKey,
      type: SettingType.Boolean
    }, // Boolean, true or false
    {
      label: 'Create Datalake Import',
      value: connection.createDatalakeImport,
      type: SettingType.Boolean
    }, // Boolean, true or false
    {
      label: 'Time Window Start',
      value: connection.timeWindowStart,
      type: SettingType.Time
    }, // Should be Time see document
    {
      label: 'Time Window Stop',
      value: connection.timeWindowStop,
      type: SettingType.Time
    }, // Should be Time see document
    {
      label: 'Time Window Timezone',
      value: connection.timeWindowTimezone,
      type: SettingType.TimeZone
    }, // Should be TimeZone see document
    {
      label: 'Seed File',
      value: connection.seedFile,
      type: SettingType.Text
    }, // Free-text (varchar 256)

    {
      label: '',
      value: '',
      type: SettingType.GroupingSpace
    }, // Layout space
    {
      label: 'Operator Notes',
      value: connection.operatorNotes,
      type: SettingType.Textarea
    }, // Free-text (varchar 256)
    {
      label: 'Contact Information',
      value: connection.contactInformation,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    {
      label: 'Description',
      value: connection.description,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    {
      label: 'Owner',
      value: connection.owner,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    // {
    //   label: 'Environment',
    //   value: connection.environment,
    //   type: SettingType.Text
    // }, // Free-text (varchar 256), skip this because functionality for it is not yet implemented in the system
    {
      label: '',
      value: '',
      type: SettingType.GroupingSpace
    }, // Layout space
    {
      label: 'Atlas Discovery',
      value: connection.atlasDiscovery,
      type: SettingType.Boolean
    }, // Boolean, true or false
    {
      label: 'Atlas Include Filter',
      value: connection.atlasIncludeFilter,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    {
      label: 'Atlas Exclude Filter',
      value: connection.atlasExcludeFilter,
      type: SettingType.Text
    }, // Free-text (varchar 256)
    {
      label: 'Atlas Last Discovery',
      value: connection.atlasLastDiscovery,
      type: SettingType.Readonly
    } // Readonly (varchar 256)
  ]

  return (
    <>
      <div>
        <div className="cards-container">
          <CardConnection
            title="Settings"
            settings={connectionSettings}
            originalData={connection}
          />
        </div>
      </div>
    </>
  )
}

export default CardsRendererConnection
