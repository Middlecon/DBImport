import Card from './Card'
import { UITable } from '../../../../utils/interfaces'
import {
  mapDisplayValue,
  nameDisplayMappings
} from '../../../../utils/nameMappings'
import {
  EtlEngine,
  EtlType,
  SettingType,
  ImportTool,
  ImportType,
  ValidationMethod
} from '../../../../utils/enums'
import './CardsRenderer.scss'

interface CardsRendererProps {
  table: UITable
}

interface Settings {
  label: string
  value: string | number | boolean
  type: SettingType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string }
  isHidden?: boolean
}

function CardsRenderer({ table }: CardsRendererProps) {
  const getEnumOptions = (key: string) => nameDisplayMappings[key] || {}

  const mainSettings: Settings[] = [
    { label: 'Database', value: table.database, type: SettingType.Readonly }, //Free-text, read-only, default selected db, potentially copyable?
    { label: 'Table', value: table.table, type: SettingType.Readonly }, // Free-text, read-only
    {
      label: 'Connection',
      value: table.connection,
      type: SettingType.Reference
    }, // Reference to /connection
    {
      label: 'Source Schema',
      value: table.sourceSchema,
      type: SettingType.Text
    }, // Free-text setting
    { label: 'Source Table', value: table.sourceTable, type: SettingType.Text }, // Free-text setting
    {
      label: 'Import Type',
      value: mapDisplayValue('importPhaseType', table.importPhaseType),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importPhaseType')
    }, // Enum mapping for 'Import Type'
    {
      label: 'ETL Type',
      value: mapDisplayValue('etlPhaseType', table.etlPhaseType),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlPhaseType')
    }, // Enum mapping for 'ETL Type'
    {
      label: 'Import Tool',
      value: mapDisplayValue('importTool', table.importTool),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('importTool')
    }, // Enum mapping for 'Import Tool'
    {
      label: 'ETL Engine',
      value: mapDisplayValue('etlEngine', table.etlEngine),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('etlEngine')
    }, // Enum mapping for 'ETL Engine'
    {
      label: 'Last Update From Source',
      value: table.lastUpdateFromSource,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Source Table Type',
      value: table.sourceTableType,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Import Database',
      value: table.importDatabase,
      type: SettingType.Text
    }, // Free-text setting
    { label: 'Import Table', value: table.importTable, type: SettingType.Text }, // Free-text setting
    {
      label: 'History Database',
      value: table.historyDatabase,
      type: SettingType.Text
    }, // Free-text setting
    {
      label: 'History Table',
      value: table.historyTable,
      type: SettingType.Text
    } // Free-text setting
  ]

  const schedule: Settings[] = [
    {
      label: 'Airflow Priority',
      value: table.airflowPriority,
      type: SettingType.Integer
    }, // Integer (should not be string in API)
    {
      label: 'Include in Airflow',
      value: table.includeInAirflow,
      type: SettingType.Boolean
    }, // Boolean
    {
      label: 'Operator Notes',
      value: table.operatorNotes,
      type: SettingType.Text
    } // Free-text setting
  ]

  const validation: Settings[] = [
    {
      label: 'Validate Import',
      value: table.validateImport,
      type: SettingType.Boolean
    }, // Boolean
    {
      label: 'Validation Method',
      value: mapDisplayValue('validationMethod', table.validationMethod),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('validationMethod')
    }, // Enum mapping for 'Validation Method'
    {
      label: 'Validate Source',
      value: mapDisplayValue('validateSource', table.validateSource),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('validateSource')
    }, // Enum mapping for 'Validate Source'
    {
      label: 'Allowed Validation Difference',
      value: table.validateDiffAllowed,
      type: SettingType.Integer
    }, // Integer
    {
      label: 'Source Row Count',
      value: table.sourceRowcount,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Source Row Count Incremental',
      value: table.sourceRowcountIncr,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Target Row Count',
      value: table.targetRowcount,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Custom Query Source SQL',
      value: table.validationCustomQuerySourceSQL,
      type: SettingType.Text,
      isConditionsMet: table.validationMethod === ValidationMethod.CustomQuery
    }, // free-text, active if validationMethod=customQuery
    {
      label: 'Custom Query Hive SQL',
      value: table.validationCustomQueryHiveSQL,
      type: SettingType.Text,
      isConditionsMet: table.validationMethod === ValidationMethod.CustomQuery
    }, // free-text, active if validationMethod=customQuery
    {
      label: 'Validate Import Table',
      value: true,
      type: SettingType.Boolean,
      isHidden: true
    }, // Always true and always hidden, should be displayed/visible in UI
    {
      label: 'Custom Query Source Value',
      value: table.validationCustomQuerySourceValue,
      type: SettingType.Readonly
    }, // Read-only setting
    {
      label: 'Custom Query Hive Value',
      value: table.validationCustomQueryHiveValue,
      type: SettingType.Readonly
    } // Read-only setting
  ]

  const importOptions: Settings[] = [
    {
      label: 'Truncate Table',
      value: table.truncateTable,
      type: SettingType.Boolean
    }, // Boolean
    {
      label: 'Allow Text Splitter',
      value: table.allowTextSplitter,
      type: SettingType.Boolean
    }, // Boolean
    {
      label: 'Force String',
      value: table.forceString,
      type: SettingType.BooleanOrDefaultFromConfig
    }, // Boolean or -1 (-1="Default from config")
    {
      label: 'Split By Column',
      value: table.splitByColumn,
      type: SettingType.Text
    }, // Free-text setting
    {
      label: 'SQL WHERE Addition',
      value: table.sqlWhereAddition,
      type: SettingType.Text
    }, // Free-text setting
    {
      label: 'Custom Query',
      value: table.customQuery,
      type: SettingType.Text,
      isConditionsMet: table.useGeneratedSql === false
    }, // Active only if useGeneratedSql=false
    {
      label: 'Custom Max Query',
      value: table.customMaxQuery,
      type: SettingType.Text,
      isConditionsMet: table.useGeneratedSql === false
    }, // Active only if useGeneratedSql=false
    {
      label: 'Use Generated SQL',
      value: table.useGeneratedSql,
      type: SettingType.Boolean
    }, // Boolean
    {
      label: 'No Merge Ingestion SQL Addition',
      value: table.nomergeIngestionSqlAddition,
      type: SettingType.Text
    }, // Free-text setting (nmore information might come about this one)
    {
      label: 'Sqoop Options',
      value: table.sqoopOptions,
      type: SettingType.Text,
      isConditionsMet: table.importTool === ImportTool.Sqoop
    }, // Free-text, active only if importTool=sqoop
    { label: 'Last Size', value: table.lastSize, type: SettingType.Readonly }, // Read-only
    { label: 'Last Rows', value: table.lastRows, type: SettingType.Readonly }, // Read-only
    {
      label: 'Last Mappers',
      value: table.lastMappers,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Generated Hive Column Definition',
      value: table.generatedHiveColumnDefinition,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Generated Sqoop Query',
      value: table.generatedSqoopQuery,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Generated Sqoop Options',
      value: table.generatedSqoopOptions,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Generated Primary Key Columns',
      value: table.generatedPkColumns,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Generated Foreign Keys',
      value: table.generatedForeignKeys,
      type: SettingType.Readonly
    } // Read-only
  ]

  const incrementalImports: Settings[] = [
    {
      label: 'Incremental Mode',
      value: mapDisplayValue('incrMode', table.incrMode),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('incrMode'),
      isConditionsMet: table.importPhaseType === ImportType.Incremental
    }, // Enum list, active if importPhaseType=incr
    {
      label: 'Incremental Column',
      value: table.incrColumn,
      type: SettingType.Text,
      isConditionsMet: table.importPhaseType === ImportType.Incremental
    }, // Free-text, active if importPhaseType=incr
    {
      label: 'Incremental Validation Method',
      value: mapDisplayValue(
        'incrValidationMethod',
        table.incrValidationMethod
      ),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('incrValidationMethod'),
      isConditionsMet: table.importPhaseType === ImportType.Incremental
    }, // Enum list, active if importPhaseType=incr
    {
      label: 'Incremental Min Value',
      value: table.incrMinvalue,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Incremental Max Value',
      value: table.incrMaxvalue,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Pending Min Value',
      value: table.incrMinvaluePending,
      type: SettingType.Readonly
    }, // Read-only
    {
      label: 'Pending Max Value',
      value: table.incrMaxvaluePending,
      type: SettingType.Readonly
    } // Read-only
  ]
  const etlOptions: Settings[] = [
    {
      label: 'Create Foreign Keys',
      value: table.createForeignKeys,
      type: SettingType.BooleanOrAuto
    }, // Boolean or Auto (-1)
    {
      label: 'Invalidate Impala',
      value: table.invalidateImpala,
      type: SettingType.BooleanOrAuto
    }, // Boolean or Auto (-1)
    {
      label: 'Soft Delete During Merge',
      value: table.softDeleteDuringMerge,
      type: SettingType.Boolean,
      isConditionsMet:
        table.etlPhaseType === EtlType.Merge ||
        table.etlPhaseType === EtlType.MergeHistoryAudit
    }, // Boolean, active only if etlPhaseType=merge or etlPhaseType=merge_history_audit
    {
      label: 'Primary Key Override',
      value: table.pkColumnOverride,
      type: SettingType.Text
    }, // Comma-separated list with columns from "columns":{}
    {
      label: 'Primary Key Override (Merge only)',
      value: table.pkColumnOverrideMergeonly,
      type: SettingType.Text
    }, // Comma-separated list with columns from "columns":{}
    {
      label: 'Merge Heap (MB)',
      value: table.mergeHeap,
      type: SettingType.Integer
    }, // Integer, value is MB
    {
      label: 'Merge Compaction Method',
      value: mapDisplayValue(
        'mergeCompactionMethod',
        table.mergeCompactionMethod
      ),
      type: SettingType.Enum,
      enumOptions: getEnumOptions('mergeCompactionMethod')
    }, // Enum mapping for 'Merge Compaction Method'
    {
      label: 'Datalake Source',
      value: table.datalakeSource,
      type: SettingType.Text
    } // Free-text setting
  ]

  const performance: Settings[] = [
    { label: 'Mappers', value: table.mappers, type: SettingType.IntegerOrAuto }, // Integer, -1 = Auto
    {
      label: 'Split Count',
      value: table.splitCount,
      type: SettingType.Integer,
      isConditionsMet: table.etlEngine === EtlEngine.Hive
    }, // Integer, active if etlEngine=hive
    {
      label: 'Spark Executor Memory',
      value: table.sparkExecutorMemory,
      type: SettingType.Text,
      isConditionsMet:
        table.etlEngine === EtlEngine.Spark ||
        table.importTool === ImportTool.Spark
    }, // Free-text, active if etlEngine or importTool=spark
    {
      label: 'Spark Executors',
      value: table.sparkExecutors,
      type: SettingType.Integer,
      isConditionsMet:
        table.etlEngine === EtlEngine.Spark ||
        table.importTool === ImportTool.Spark
    } // Integer, active if etlEngine or importTool=spark
  ]

  const siteToSiteCopy: Settings[] = [
    {
      label: 'Copy Finished',
      value: table.copyFinished,
      type: SettingType.Readonly
    }, // Read-only, timestamp
    { label: 'Copy Slave', value: table.copySlave, type: SettingType.Readonly } // Read-only, Boolean
  ]

  return (
    <>
      <div className="cards">
        <div className="cards-container">
          <Card title="Main Settings" settings={mainSettings} />
          <Card title="Performance" settings={performance} />
          <Card title="Validation" settings={validation} />
          <Card title="Schedule" settings={schedule} />
          <Card title="Site-to-site Copy" settings={siteToSiteCopy} />
        </div>
        <div className="cards-container">
          <Card title="Import Options" settings={importOptions} />
          <Card title="ETL Options" settings={etlOptions} />
          <Card title="Incremental Imports" settings={incrementalImports} />
        </div>
      </div>
      <div className="cards-narrow">
        <div className="cards-container">
          <Card title="Main Settings" settings={mainSettings} />
          <Card title="Import Options" settings={importOptions} />
          <Card title="ETL Options" settings={etlOptions} />
          <Card
            title="Incremental Imports"
            settings={incrementalImports}
            isNotEditable={table.importPhaseType !== 'incremental'}
            isDisabled={table.importPhaseType !== 'incremental'}
          />
          <Card title="Performance" settings={performance} />
          <Card title="Validation" settings={validation} />
          <Card title="Schedule" settings={schedule} />
          <Card
            title="Site-to-site Copy"
            settings={siteToSiteCopy}
            isNotEditable={true}
          />
        </div>
      </div>
    </>
  )
}

export default CardsRenderer
