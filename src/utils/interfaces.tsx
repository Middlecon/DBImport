export interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
}

export interface Table {
  connection: string
  database: string
  etlEngine: string
  etlPhaseType: string
  importPhaseType: string
  importTool: string
  lastUpdateFromSource: string
  sourceSchema: string
  sourceTable: string
  table: string
}

export interface Column {
  header: string
  accessor?: keyof Table
  isAction?: boolean
}
