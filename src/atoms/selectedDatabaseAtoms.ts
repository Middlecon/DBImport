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
