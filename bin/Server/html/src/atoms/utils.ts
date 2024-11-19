import { getDefaultStore } from 'jotai'
import { atomWithStorage, RESET } from 'jotai/utils'
import {
  importPersistStateAtom,
  importTableListFiltersAtom,
  connectionFilterAtom,
  airflowImportFilterAtom,
  airflowExportFilterAtom,
  airflowCustomFilterAtom,
  isAirflowSubmenuActiveAtom,
  usernameAtom,
  exportCnListFiltersAtom,
  selectedExportConnectionAtom,
  isConfigurationSubmenuActiveAtom,
  latestPathAtom
} from './atoms'

export function atomWithSessionStorage<T>(key: string, initialValue: T) {
  return atomWithStorage<T>(key, initialValue, {
    getItem: (key) => {
      const storedValue = sessionStorage.getItem(key)
      if (storedValue) {
        return JSON.parse(storedValue)
      }
      return initialValue
    },
    setItem: (key, newValue) => {
      sessionStorage.setItem(key, JSON.stringify(newValue))
    },
    removeItem: (key) => {
      sessionStorage.removeItem(key)
    }
  })
}

export const clearSessionStorageAtoms = () => {
  const store = getDefaultStore()

  store.set(importPersistStateAtom, RESET)
  store.set(selectedExportConnectionAtom, RESET)
  store.set(importTableListFiltersAtom, RESET)
  store.set(exportCnListFiltersAtom, RESET)
  store.set(connectionFilterAtom, RESET)
  store.set(airflowImportFilterAtom, RESET)
  store.set(airflowExportFilterAtom, RESET)
  store.set(airflowCustomFilterAtom, RESET)
  store.set(isAirflowSubmenuActiveAtom, RESET)
  store.set(isConfigurationSubmenuActiveAtom, RESET)
  store.set(usernameAtom, RESET)
  store.set(latestPathAtom, RESET)

  const sessionKeys = [
    'selectedImportDatabase',
    'selectedExportConnection',
    'importDbListFilters',
    'exportCnListFilters',
    'connectionFilters',
    'airflowImportFilter',
    'airflowExportFilter',
    'airflowCustomFilter',
    'isAirflowSubmenuActive',
    'username',
    'latestPath'
  ]
  sessionKeys.forEach((key) => sessionStorage.removeItem(key))
}
