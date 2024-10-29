import { getDefaultStore } from 'jotai'
import { atomWithStorage, RESET } from 'jotai/utils'
import {
  selectedImportDatabaseAtom,
  importDbListFiltersAtom,
  connectionFilterAtom,
  airflowImportFilterAtom,
  airflowExportFilterAtom,
  airflowCustomFilterAtom,
  isAirflowSubmenuActiveAtom,
  usernameAtom
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

  store.set(selectedImportDatabaseAtom, RESET)
  store.set(importDbListFiltersAtom, RESET)
  store.set(connectionFilterAtom, RESET)
  store.set(airflowImportFilterAtom, RESET)
  store.set(airflowExportFilterAtom, RESET)
  store.set(airflowCustomFilterAtom, RESET)
  store.set(isAirflowSubmenuActiveAtom, RESET)
  store.set(usernameAtom, RESET)

  const sessionKeys = [
    'selectedImportDatabase',
    'importDbListFilters',
    'connectionFilters',
    'airflowImportFilter',
    'airflowExportFilter',
    'airflowCustomFilter',
    'isAirflowSubmenuActive',
    'username'
  ]
  sessionKeys.forEach((key) => sessionStorage.removeItem(key))
}
