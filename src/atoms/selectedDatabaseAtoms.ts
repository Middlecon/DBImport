import { atom } from 'jotai'
import { atomWithSessionStorage } from './utils'

export const selectedImportDatabaseAtom = atomWithSessionStorage<string | null>(
  'selectedImportDatabase',
  null
)

export const isDbDropdownReadyAtom = atom(false)
