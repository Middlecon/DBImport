import React from 'react'
import Card from './Card'
import { UITable } from '../../../../utils/interfaces'
import {
  mapDisplayValue,
  nameDisplayMappings
} from '../../../../utils/nameMappings'
import { FieldType, ValidationMethod } from '../../../../utils/enums'

interface CardsRendererProps {
  table: UITable
}

interface Fields {
  label: string
  value: string | number | boolean
  type: FieldType
  isConditionsMet?: boolean
  enumOptions?: { [key: string]: string }
  isHidden?: boolean
}

const CardsRenderer: React.FC<CardsRendererProps> = ({ table }) => {
  const getEnumOptions = (key: string) => nameDisplayMappings[key] || {}

  const mainSettings: Fields[] = [
    { label: 'Database', value: table.database, type: FieldType.Text }, // Database, read-only, potentially copyable
    { label: 'Table', value: table.table, type: FieldType.Readonly }, // Table, read-only
    { label: 'Connection', value: table.connection, type: FieldType.Text }, // Reference to /connection
    { label: 'Source Schema', value: table.sourceSchema, type: FieldType.Text }, // Free-text field
    { label: 'Source Table', value: table.sourceTable, type: FieldType.Text }, // Free-text field
    {
      label: 'Import Type',
      value: mapDisplayValue('importPhaseType', table.importPhaseType),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('importPhaseType')
    }, // Enum mapping for 'Import Type'
    {
      label: 'ETL Type',
      value: mapDisplayValue('etlPhaseType', table.etlPhaseType),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('etlPhaseType')
    }, // Enum mapping for 'ETL Type'
    {
      label: 'Import Tool',
      value: mapDisplayValue('importTool', table.importTool),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('importTool')
    }, // Enum mapping for 'Import Tool'
    {
      label: 'ETL Engine',
      value: mapDisplayValue('etlEngine', table.etlEngine),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('etlEngine')
    }, // Enum mapping for 'ETL Engine'
    {
      label: 'Last Update From Source',
      value: table.lastUpdateFromSource,
      type: FieldType.Readonly
    }, // Read-only date field
    {
      label: 'Source Table Type',
      value: table.sourceTableType,
      type: FieldType.Readonly
    }, // Read-only field
    {
      label: 'Import Database',
      value: table.importDatabase,
      type: FieldType.Text
    }, // Free-text field
    { label: 'Import Table', value: table.importTable, type: FieldType.Text }, // Free-text field
    {
      label: 'History Database',
      value: table.historyDatabase,
      type: FieldType.Text
    }, // Free-text field
    { label: 'History Table', value: table.historyTable, type: FieldType.Text } // Free-text field
  ]

  const schedule: Fields[] = [
    {
      label: 'Airflow Priority',
      value: table.airflowPriority,
      type: FieldType.Integer
    }, // Integer (not string in API)
    {
      label: 'Include in Airflow',
      value: table.includeInAirflow,
      type: FieldType.Boolean
    }, // Boolean
    {
      label: 'Operator Notes',
      value: table.operatorNotes,
      type: FieldType.Text
    } // Free-text field
  ]

  const validation: Fields[] = [
    {
      label: 'Validate Import',
      value: table.validateImport,
      type: FieldType.Boolean
    }, // Boolean
    {
      label: 'Validation Method',
      // value: mapDisplayValue('validationMethod', table.validationMethod),
      value: ValidationMethod.CustomQuery,

      type: FieldType.Enum,
      enumOptions: getEnumOptions('validationMethod')
    }, // Enum mapping for 'Validation Method'
    {
      label: 'Validate Source',
      value: mapDisplayValue('validateSource', table.validateSource),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('validateSource')
    }, // Enum mapping for 'Validate Source'
    {
      label: 'Allowed Validation Difference',
      value: table.validateDiffAllowed,
      type: FieldType.Integer
    }, // Integer
    {
      label: 'Source Row Count',
      value: table.sourceRowcount,
      type: FieldType.Readonly
    }, // Read-only field
    {
      label: 'Source Row Count Incremental',
      value: table.sourceRowcountIncr,
      type: FieldType.Readonly
    }, // Read-only field
    {
      label: 'Target Row Count',
      value: table.targetRowcount,
      type: FieldType.Readonly
    }, // Read-only field
    {
      label: 'Custom Query for Source SQL',
      value: table.validationCustomQuerySourceSQL,
      type: FieldType.Text,
      isConditionsMet: table.validationMethod === ValidationMethod.CustomQuery
    }, // isConditionsMetally active free-text
    {
      label: 'Custom Query for Hive SQL',
      value: table.validationCustomQueryHiveSQL,
      type: FieldType.Text
      // isConditionsMet: "validationMethod === 'customQuery'"
    }, // isConditionsMetally active free-text
    {
      label: 'Validate Import Table',
      value: true,
      type: FieldType.Boolean,
      isHidden: true
    }, // Always true, hidden
    {
      label: 'Custom Query Source Value',
      value: table.validationCustomQuerySourceValue,
      type: FieldType.Readonly
    }, // Read-only field
    {
      label: 'Custom Query Hive Value',
      value: table.validationCustomQueryHiveValue,
      type: FieldType.Readonly
    } // Read-only field
  ]

  const importOptions: Fields[] = [
    {
      label: 'Truncate Table',
      value: table.truncateTable,
      type: FieldType.Boolean
    }, // Boolean
    {
      label: 'Allow Text Splitter',
      value: table.allowTextSplitter,
      type: FieldType.Boolean
    }, // Boolean
    {
      label: 'Force String',
      value: table.forceString,
      type: FieldType.BooleanOrDefaultFromConfig
    }, // Boolean or -1 (default from config)
    {
      label: 'Split By Column',
      value: table.splitByColumn,
      type: FieldType.Text
    }, // Free-text field
    {
      label: 'SQL WHERE Addition',
      value: table.sqlWhereAddition,
      type: FieldType.Text
    }, // Free-text field
    {
      label: 'Custom Query',
      value: table.customQuery,
      type: FieldType.Text
      // isConditionsMet: 'useGeneratedSql === false'
    }, // Active only if useGeneratedSql=false
    {
      label: 'Custom Max Query',
      value: table.customMaxQuery,
      type: FieldType.Text
      // isConditionsMet: 'useGeneratedSql === false'
    }, // Active only if useGeneratedSql=false
    {
      label: 'Use Generated SQL',
      value: table.useGeneratedSql,
      type: FieldType.Boolean
    }, // Boolean
    {
      label: 'No Merge Ingestion SQL Addition',
      value: table.nomergeIngestionSqlAddition,
      type: FieldType.Text
    }, // Free-text field (not sure where this is used)
    {
      label: 'Sqoop Options',
      value: table.sqoopOptions,
      type: FieldType.Text
      // isConditionsMet: "importTool === 'sqoop'"
    }, // Free-text, active only if importTool=sqoop
    { label: 'Last Size', value: table.lastSize, type: FieldType.Readonly }, // Read-only
    { label: 'Last Rows', value: table.lastRows, type: FieldType.Readonly }, // Read-only
    {
      label: 'Last Mappers',
      value: table.lastMappers,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Generated Hive Column Definition',
      value: table.generatedHiveColumnDefinition,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Generated Sqoop Query',
      value: table.generatedSqoopQuery,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Generated Sqoop Options',
      value: table.generatedSqoopOptions,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Generated Primary Key Columns',
      value: table.generatedPkColumns,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Generated Foreign Keys',
      value: table.generatedForeignKeys,
      type: FieldType.Readonly
    } // Read-only
  ]

  const incrementalImports: Fields[] = [
    {
      label: 'Incremental Mode',
      value: mapDisplayValue('incrMode', table.incrMode),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('incrMode')
      // isConditionsMet: "importPhaseType === 'incr'"
    }, // Enum list, active if importPhaseType=incr
    {
      label: 'Incremental Column',
      value: table.incrColumn,
      type: FieldType.Text
      // isConditionsMet: "importPhaseType === 'incr'"
    }, // Free-text, active if importPhaseType=incr
    {
      label: 'Incremental Validation Method',
      value: mapDisplayValue(
        'incrValidationMethod',
        table.incrValidationMethod
      ),
      type: FieldType.Enum,
      enumOptions: getEnumOptions('incrValidationMethod')
      // isConditionsMet: "importPhaseType === 'incr'"
    }, // Enum list, active if importPhaseType=incr
    {
      label: 'Incremental Min Value',
      value: table.incrMinvalue,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Incremental Max Value',
      value: table.incrMaxvalue,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Pending Min Value',
      value: table.incrMinvaluePending,
      type: FieldType.Readonly
    }, // Read-only
    {
      label: 'Pending Max Value',
      value: table.incrMaxvaluePending,
      type: FieldType.Readonly
    } // Read-only
  ]
  const etlOptions: Fields[] = [
    {
      label: 'Create Foreign Keys',
      value: table.createForeignKeys,
      type: FieldType.BooleanOrAuto
    }, // Boolean or Auto (-1)
    {
      label: 'Invalidate Impala',
      value: table.invalidateImpala,
      type: FieldType.BooleanOrAuto
    }, // Boolean or Auto (-1)
    {
      label: 'Soft Delete During Merge',
      value: table.softDeleteDuringMerge,
      type: FieldType.Boolean
      // isConditionsMet:
      //   "etlPhaseType === 'merge' || etlPhaseType === 'merge_history_audit'"
    }, // Boolean, active only for merge types
    {
      label: 'Primary Key Override',
      value: table.pkColumnOverride,
      type: FieldType.Text
    }, // Comma-separated list
    {
      label: 'Primary Key Override (Merge only)',
      value: table.pkColumnOverrideMergeonly,
      type: FieldType.Text
    }, // Comma-separated list
    {
      label: 'Merge Heap (MB)',
      value: table.mergeHeap,
      type: FieldType.Integer
    }, // Integer in MB
    {
      label: 'Merge Compaction Method',
      value: mapDisplayValue(
        'mergeCompactionMethod',
        table.mergeCompactionMethod
      ),
      type: FieldType.Enum
    }, // Enum list
    {
      label: 'Datalake Source',
      value: table.datalakeSource,
      type: FieldType.Text
    } // Free-text field
  ]

  const performance: Fields[] = [
    { label: 'Mappers', value: table.mappers, type: FieldType.IntegerOrAuto }, // Integer, -1 = Auto
    {
      label: 'Split Count',
      value: table.splitCount,
      type: FieldType.Integer
      // isConditionsMet: "etlEngine === 'hive'"
    }, // Integer, active if etlEngine=hive
    {
      label: 'Spark Executor Memory',
      value: table.sparkExecutorMemory,
      type: FieldType.Text
      // isConditionsMet: "etlEngine === 'spark' || ´importTool === 'spark'"
    }, // Free-text, active if etlEngine or importTool=spark
    {
      label: 'Spark Executors',
      value: table.sparkExecutors,
      type: FieldType.Integer
      // isConditionsMet: "etlEngine === 'spark' || importTool === 'spark'"
    } // Integer, active if etlEngine or importTool=spark
  ]

  const siteToSiteCopy: Fields[] = [
    {
      label: 'Copy Finished',
      value: table.copyFinished,
      type: FieldType.Readonly
    }, // Read-only timestamp
    { label: 'Copy Slave', value: table.copySlave, type: FieldType.Readonly } // Read-only Boolean
  ]

  return (
    <div className="cards">
      <Card title="Main Settings" fields={mainSettings} />
      <Card title="Schedule" fields={schedule} />
      <Card title="Validation" fields={validation} />
      <Card title="Import Options" fields={importOptions} />
      <Card title="Incremental Imports" fields={incrementalImports} />
      <Card title="ETL Options" fields={etlOptions} />
      <Card title="Performance" fields={performance} />
      <Card title="Site-to-site Copy" fields={siteToSiteCopy} />
    </div>
  )
}

export default CardsRenderer
