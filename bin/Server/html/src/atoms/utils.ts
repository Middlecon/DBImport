import { atomWithStorage } from 'jotai/utils'

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
