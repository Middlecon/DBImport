import { atom, getDefaultStore } from 'jotai'
import { atomWithSessionStorage } from './utils'

export const selectedImportDatabaseAtom = atomWithSessionStorage<string | null>(
  'selectedImportDatabase',
  null
)

export const selectedExportConnectionAtom = atomWithSessionStorage<
  string | null
>('selectedExportConnection', null)

export const isDbDropdownReadyAtom = atom(false)

export const importDbListFiltersAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('importDbListFilters', {})

export const exportCnListFiltersAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('exportCnListFilters', {})

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
export const isConfigurationSubmenuActiveAtom = atomWithSessionStorage<boolean>(
  'isConfigurationSubmenuActive',
  false
)

export const airflowTypeAtom = atom('')

export const usernameAtom = atomWithSessionStorage<string | null>(
  'username',
  null
)

export const latestPathAtom = atomWithSessionStorage<string | null>(
  'latestPath',
  null
)

export const setLatestPath = (value: string) => {
  const store = getDefaultStore()
  store.set(latestPathAtom, value)
}
