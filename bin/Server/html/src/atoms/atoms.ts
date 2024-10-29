import { atom } from 'jotai'
import { atomWithSessionStorage } from './utils'

export const selectedImportDatabaseAtom = atomWithSessionStorage<string | null>(
  'selectedImportDatabase',
  null
)

export const isDbDropdownReadyAtom = atom(false)

export const importDbListFiltersAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('importDbListFilters', {})

export const connectionFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('connectionFilters', {})

export const airflowImportFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowImportFilter', {})

export const airflowExportFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowExportFilter', {})

export const airflowCustomFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowCustomFilter', {})

export const isAirflowSubmenuActiveAtom = atomWithSessionStorage<boolean>(
  'isAirflowSubmenuActive',
  false
)

export const airflowTypeAtom = atom('')
